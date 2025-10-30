package bronze

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.nio.file.{Files, Paths}

/**
 * YellowTripdataBronzeApp
 * ------------------------------------------------------------
 * Purpose:
 *   - Read the Taxi Zone CSV from the raw zone.
 *   - Light normalization of a few text columns (trim -> null).
 *   - Run simple, schema-tolerant DQ checks (nullability/type-like checks).
 *   - Produce metrics (row counts and null % per key column).
 *   - Write all outputs to the bronze zone as CSV, overwriting prior runs.
 *
 * Notes:
 *   - "Schema tolerant" means: if a required column isn't present in the input,
 *     we still add the DQ flag column and set it to TRUE so the job doesn't fail.
 *   - We coalesce(1) for metrics to produce a single CSV per folder for convenience.
 */
object YellowTripdataBronzeApp {

  def main(args: Array[String]): Unit = {
    // Build a local SparkSession that uses all CPU cores on the machine.
    // Implicit makes it available to helper functions that require it.
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("BronzeApp")
      .master("local[*]")
      .getOrCreate()

    // ------------------------------------------------------------
    // I/O paths
    // ------------------------------------------------------------
    // Choose the first existing input path from a list of candidates.
    // This helps if the file is sometimes checked in under a different folder.
    val inputPath  = firstExisting(
      Seq(
        "data/raw/taxi_zone.csv",                // primary path (what your repo shows)
        "data/raw/taxi_zones/taxi_zones.csv"     // fallback if you rename later
      )
    )
    // All outputs for this job go into the bronze_taxi_zone folder.
    val outputPath = "data/bronze/bronze_taxi_zone"

    // ------------------------------------------------------------
    // CSV schema definition
    // ------------------------------------------------------------
    // We declare a fixed schema to avoid per-job inference differences and
    // to ensure consistent downstream typing. All columns are nullable here
    // because raw data may contain blanks/missing values.
    val taxiSchema = StructType(Seq(
      StructField("LocationID",    StringType,  nullable = true),
      StructField("Borough",       StringType,  nullable = true),
      StructField("Zone",          StringType,  nullable = true),
      StructField("service_zone",  StringType,  nullable = true)
    ))

    // ------------------------------------------------------------
    // Step 1: Read input CSV (with header)
    // ------------------------------------------------------------
    // We pass the schema explicitly and set header=true so Spark treats the
    // first line as column names, not data.
    val raw = spark.read
      .option("header", "true")
      .schema(taxiSchema)
      .csv(inputPath)

    println(s"Reading: $inputPath")
    println("Schema of input data:")
    raw.printSchema()

    // ------------------------------------------------------------
    // Step 2: Normalize and clean
    // ------------------------------------------------------------
    // We trim leading/trailing whitespace and convert empty strings to null
    // for each "key column" we care about. If a column is missing, we skip it.
    val keyCols  = Seq("LocationID", "Borough", "Zone", "service_zone")
    val cleaned  = normalizeColumns(raw, keyCols)

    // ------------------------------------------------------------
    // Step 3: Apply DQ checks
    // ------------------------------------------------------------
    // For each check we define:
    //   - a name (column to add, starting with "chk_")
    //   - required input columns
    //   - a predicate (DataFrame => Column) that returns a boolean expression
    // If required columns are present, we compute the check; otherwise we add
    // the check column as TRUE so the pipeline doesn’t fail on schema drift.
    val checked  = applyChecks(cleaned)
    println("Available columns after cleaning: " + checked.columns.mkString(", "))

    // ------------------------------------------------------------
    // Step 4: Metrics
    // ------------------------------------------------------------
    // summarizeChecks produces row_count_total/pass/reject based on all chk_* flags.
    // computeNullPercents produces null_pct_<col> per column in keyCols.
    val summary     = summarizeChecks(checked)
    val nullSummary = computeNullPercents(checked, keyCols)

    // ------------------------------------------------------------
    // Step 5: Writes (all CSV)
    // ------------------------------------------------------------
    // We write three outputs:
    //   1) cleaned + DQ columns (full_dq_checked)
    //   2) counts summary (run_summary_csv)
    //   3) null % summary (null_summary_csv)
    //
    // Each uses mode("overwrite") for re-runnability.
    writeCSVIfRows(checked,       s"$outputPath/full_dq_checked")
    writeCSVIfSchema(summary,     s"$outputPath/run_summary_csv")
    writeCSVIfSchema(nullSummary, s"$outputPath/null_summary_csv")

    println(s"CSV outputs written (if non-empty) under: $outputPath")
    spark.stop()
  }

  // ============================================================
  // Helper functions
  // ============================================================

  /**
   * Pick the first input path that exists, or throw with a helpful error.
   * This avoids brittle "file not found" errors when you move CSVs around.
   */
  private def firstExisting(candidates: Seq[String]): String = {
    candidates.find(p => Files.exists(Paths.get(p)))
      .getOrElse(throw new IllegalArgumentException(
        s"""Input file not found. Tried:
           |${candidates.mkString(" - ", "\n - ", "")}
           |Run from the project root so relative paths resolve.
           |""".stripMargin))
  }

  /**
   * Normalize text columns:
   * - trim() to remove leading/trailing spaces
   * - empty string -> NULL
   * If a column is absent in the DataFrame, it is ignored (schema-tolerant).
   */
  private def normalizeColumns(df: DataFrame, cols: Seq[String]): DataFrame =
    cols.foldLeft(df) { (acc, c) =>
      if (acc.columns.contains(c))
        acc.withColumn(
          c,
          when(trim(col(c)) === "" || col(c).isNull, lit(null:String)) // convert blanks to NULL
            .otherwise(trim(col(c)).cast(StringType))                  // keep as trimmed String
        )
      else acc
    }

  /**
   * Define and apply simple DQ checks.
   * Each check returns a boolean column "chk_<name>" where TRUE means the row passes.
   * Current checks:
   *  - LocationID must look numeric (cast to Int is not null) OR be null
   *  - Borough must be non-null (after normalization)
   *  - Zone must be non-null
   *  - service_zone must be non-null
   *
   * If required columns aren't present, we still add the check as TRUE to avoid failures.
   */
  private def applyChecks(df: DataFrame): DataFrame = {
    val checkDefs: Seq[(String, Seq[String], DataFrame => Column)] = Seq(
      // Passes when LocationID is either null OR can be cast to integer.
      ("chk_LocationID_numeric", Seq("LocationID"),
        d => d("LocationID").cast(IntegerType).isNotNull || d("LocationID").isNull),

      // Borough cannot be null (post-normalization).
      ("chk_Borough_not_null", Seq("Borough"),
        d => d("Borough").isNotNull),

      // Zone cannot be null.
      ("chk_Zone_not_null", Seq("Zone"),
        d => d("Zone").isNotNull),

      // service_zone cannot be null.
      ("chk_service_zone_not_null", Seq("service_zone"),
        d => d("service_zone").isNotNull)
    )

    // Fold over the definitions and add each "chk_*" column.
    checkDefs.foldLeft(df) { case (tmp, (name, reqCols, cond)) =>
      if (reqCols.forall(tmp.columns.contains)) tmp.withColumn(name, cond(tmp))
      else tmp.withColumn(name, lit(true)) // schema-tolerant: if inputs missing, consider it passed
    }
  }

  /**
   * Summarize DQ results (row counts):
   * - If no chk_* columns exist, we only return row_count_total.
   * - Otherwise, pass = rows where all chk_* are TRUE; reject = total - pass.
   * Output schema: (metric STRING, value STRING, run_ts TIMESTAMP)
   */
  private def summarizeChecks(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val checkCols = df.columns.filter(_.startsWith("chk_"))

    if (checkCols.isEmpty) {
      val total = df.count()
      return Seq(("row_count_total", total.toString, new java.sql.Timestamp(System.currentTimeMillis())))
        .toDF("metric", "value", "run_ts")
    }

    val total  = df.count()
    val pass   = df.filter(checkCols.map(col).reduce(_ && _)).count()
    val reject = total - pass

    Seq(
      ("row_count_total",  total.toString),
      ("row_count_pass",   pass.toString),
      ("row_count_reject", reject.toString)
    ).toDF("metric", "value").withColumn("run_ts", current_timestamp())
  }

  /**
   * Compute null percentage per column in `cols`.
   * - Skips columns that are not present.
   * - value is stored as STRING for easier CSV export.
   * Output schema: (metric STRING, value STRING, run_ts TIMESTAMP)
   */
  private def computeNullPercents(df: DataFrame, cols: Seq[String])
                                 (implicit spark: SparkSession): DataFrame = {
    val pieces = cols.filter(df.columns.contains).map { c =>
      df.agg(
        (sum(when(col(c).isNull || col(c) === "", 1).otherwise(0)).cast("double") / count(lit(1))).as("v")
      ).select(lit(s"null_pct_$c").as("metric"), col("v").cast(StringType).as("value"))
       .withColumn("run_ts", current_timestamp())
    }

    // If nothing was computed (e.g., none of the columns exist), return an empty metrics DF.
    pieces.reduceOption(_.unionByName(_)).getOrElse(emptyMetricsDF)
  }

  /**
   * Produce an empty metrics DataFrame with the right schema for the summaries.
   * Useful when no inputs match and we still want a consistent type.
   */
  private def emptyMetricsDF(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq.empty[(String, String, java.sql.Timestamp)]
      .toDF("metric", "value", "run_ts")
  }

  /**
   * Write CSV only if there is at least one row.
   * Used for data outputs to avoid creating empty folders/files.
   */
  private def writeCSVIfRows(df: DataFrame, path: String): Unit = {
    if (!df.head(1).isEmpty)
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    else println(s"Skipped CSV write (no rows): $path")
  }

  /**
   * Write CSV if DataFrame has a non-empty schema.
   * Used for metrics where the DF may be empty but the schema exists.
   */
  private def writeCSVIfSchema(df: DataFrame, path: String): Unit = {
    if (df.schema.nonEmpty)
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    else println(s"Skipped CSV write (empty schema): $path")
  }
}