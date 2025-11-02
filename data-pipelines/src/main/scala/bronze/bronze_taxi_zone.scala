package bronze

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}

/**
 * ============================================================
 * YellowTripdataBronzeApp
 * ------------------------------------------------------------
 * Purpose:
 *   - Read the Taxi Zone CSV from the raw zone.
 *   - Light normalization of text columns (trim -> null).
 *   - Run schema-tolerant DQ checks (nullability/type-like checks).
 *   - Produce metrics (row counts and null % per key column).
 *   - Write outputs to the bronze zone as Parquet files.
 *
 * Notes:
 *   - "Schema tolerant" means: if a required column isn't present,
 *     we still add the DQ flag column (TRUE) so the job doesn’t fail.
 *   - coalesce(1) used for metrics to simplify output structure.
 * ============================================================
 */
object YellowTripdataBronzeApp {

  def main(args: Array[String]): Unit = {
    // ------------------------------------------------------------
    // SparkSession setup
    // ------------------------------------------------------------
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("BronzeApp")
      .master("local[*]")
      .getOrCreate()

    // ------------------------------------------------------------
    // I/O paths
    // ------------------------------------------------------------
    val inputPath = firstExisting(
      Seq(
        "data/raw/taxi_zone.csv",               // primary path
        "data/raw/taxi_zones/taxi_zones.csv"    // fallback if renamed
      )
    )
    val outputPath = "data/bronze/bronze_taxi_zone"

    // ------------------------------------------------------------
    // Schema definition
    // ------------------------------------------------------------
    val taxiSchema = StructType(Seq(
      StructField("LocationID", StringType, nullable = true),
      StructField("Borough", StringType, nullable = true),
      StructField("Zone", StringType, nullable = true),
      StructField("service_zone", StringType, nullable = true)
    ))

    // ------------------------------------------------------------
    // Step 1: Read input CSV
    // ------------------------------------------------------------
    val raw = spark.read
      .option("header", "true")
      .schema(taxiSchema)
      .csv(inputPath)

    println(s"Reading: $inputPath")
    raw.printSchema()

    // ------------------------------------------------------------
    // Step 2: Normalize and clean
    // ------------------------------------------------------------
    val keyCols = Seq("LocationID", "Borough", "Zone", "service_zone")
    val cleaned = normalizeColumns(raw, keyCols)

    // ------------------------------------------------------------
    // Step 3: Apply DQ checks
    // ------------------------------------------------------------
    val checked = applyChecks(cleaned)
    println("Available columns after cleaning: " + checked.columns.mkString(", "))

    // ------------------------------------------------------------
    // Step 4: Compute metrics
    // ------------------------------------------------------------
    val summary = summarizeChecks(checked)
    val nullSummary = computeNullPercents(checked, keyCols)

    // ------------------------------------------------------------
    // Step 5: Write outputs (now as Parquet)
    // ------------------------------------------------------------
    writeParquetIfRows(checked, s"$outputPath/full_dq_checked")
    writeParquetIfSchema(summary, s"$outputPath/run_summary_parquet")
    writeParquetIfSchema(nullSummary, s"$outputPath/null_summary_parquet")

    println(s"Parquet outputs written (if non-empty) under: $outputPath")
    spark.stop()
  }

  // ============================================================
  // Helper methods
  // ============================================================

  /** Return the first existing path from a list of candidates. */
  private def firstExisting(candidates: Seq[String]): String = {
    candidates.find(p => Files.exists(Paths.get(p)))
      .getOrElse(throw new IllegalArgumentException(
        s"""Input file not found. Tried:
           |${candidates.mkString(" - ", "\n - ", "")}
           |Run from the project root so relative paths resolve.
           |""".stripMargin))
  }

  /** Normalize text columns (trim, empty → NULL). */
  private def normalizeColumns(df: DataFrame, cols: Seq[String]): DataFrame = {
    cols.foldLeft(df) { (acc, c) =>
      if (acc.columns.contains(c))
        acc.withColumn(
          c,
          when(trim(col(c)) === "" || col(c).isNull, lit(null:String))
            .otherwise(trim(col(c)).cast(StringType))
        )
      else acc
    }
  }

  /** Apply schema-tolerant DQ checks, adding chk_* columns. */
  private def applyChecks(df: DataFrame): DataFrame = {
    val checkDefs: Seq[(String, Seq[String], DataFrame => Column)] = Seq(
      ("chk_LocationID_numeric", Seq("LocationID"),
        d => d("LocationID").cast(IntegerType).isNotNull || d("LocationID").isNull),
      ("chk_Borough_not_null", Seq("Borough"),
        d => d("Borough").isNotNull),
      ("chk_Zone_not_null", Seq("Zone"),
        d => d("Zone").isNotNull),
      ("chk_service_zone_not_null", Seq("service_zone"),
        d => d("service_zone").isNotNull)
    )

    checkDefs.foldLeft(df) { case (tmp, (name, reqCols, cond)) =>
      if (reqCols.forall(tmp.columns.contains)) tmp.withColumn(name, cond(tmp))
      else tmp.withColumn(name, lit(true))
    }
  }

  /** Summarize DQ results with total/pass/reject counts. */
  private def summarizeChecks(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val checkCols = df.columns.filter(_.startsWith("chk_"))

    if (checkCols.isEmpty) {
      val total = df.count()
      return Seq(("row_count_total", total.toString, new java.sql.Timestamp(System.currentTimeMillis())))
        .toDF("metric", "value", "run_ts")
    }

    val total = df.count()
    val pass = df.filter(checkCols.map(col).reduce(_ && _)).count()
    val reject = total - pass

    Seq(
      ("row_count_total", total.toString),
      ("row_count_pass", pass.toString),
      ("row_count_reject", reject.toString)
    ).toDF("metric", "value").withColumn("run_ts", current_timestamp())
  }

  /** Compute null percentages for listed columns. */
  private def computeNullPercents(df: DataFrame, cols: Seq[String])
                                 (implicit spark: SparkSession): DataFrame = {
    val pieces = cols.filter(df.columns.contains).map { c =>
      df.agg(
        (sum(when(col(c).isNull || col(c) === "", 1).otherwise(0)).cast("double") / count(lit(1))).as("v")
      ).select(lit(s"null_pct_$c").as("metric"), col("v").cast(StringType).as("value"))
        .withColumn("run_ts", current_timestamp())
    }

    pieces.reduceOption(_.unionByName(_)).getOrElse(emptyMetricsDF)
  }

  /** Return empty metrics DataFrame with the expected schema. */
  private def emptyMetricsDF(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq.empty[(String, String, java.sql.Timestamp)]
      .toDF("metric", "value", "run_ts")
  }

  /** Write Parquet only if DataFrame has at least one row. */
  private def writeParquetIfRows(df: DataFrame, path: String): Unit = {
    if (!df.head(1).isEmpty)
      df.coalesce(1).write.mode("overwrite").parquet(path)
    else println(s"Skipped Parquet write (no rows): $path")
  }

  /** Write Parquet only if DataFrame has a non-empty schema. */
  private def writeParquetIfSchema(df: DataFrame, path: String): Unit = {
    if (df.schema.nonEmpty)
      df.coalesce(1).write.mode("overwrite").parquet(path)
    else println(s"Skipped Parquet write (empty schema): $path")
  }
}