package bronze

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * ============================================================
 *  YellowTripdataBronzeTripsApp
 * ------------------------------------------------------------
 * PURPOSE:
 *   Bronze-layer processing of NYC Yellow Taxi Trip Data.
 *   - Read raw Parquet.
 *   - Apply DQ validations: type checks + domain/range checks.
 *   - Split into pass / reject.
 *   - Write outputs and summary metrics to Parquet.
 *
 * OUTPUTS (under bronze_yellow_tripdata/):
 *   pass/                          valid records
 *   _rejects/                      invalid records
 *   _run_summary_counts/           total / pass / reject counts
 *   _run_summary_nulls/            null% per column
 *   _run_summary_checks/           per-check pass/fail metrics
 *   _run_summary_domain_range/     aggregate domain/range metric
 *   _run_summary_all/              union of all summaries
 *
 * Author:  Zachary Gacer
 * Date:    October 2025
 * ============================================================
 */
object YellowTripdataBronzeTripsApp {

  def main(args: Array[String]): Unit = {

    // 1) Spark session
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("BronzeApp")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // 2) IO paths
    val inputPath  = "data/raw/yellow_tripdata_2025-01.parquet"
    val outputPath = "data/bronze/bronze_yellow_tripdata"

    // 3) Read raw
    val raw = spark.read.parquet(inputPath)
    println("Loaded input schema:")
    raw.printSchema()

    // 4) Light normalization
    val cleaned = normalizeStringCols(raw, Seq("store_and_fwd_flag"))

    // 5) DQ checks
    val typed     = applyChecks(cleaned)                 // type conformance flags
    val withRules = addDomainAndRangeChecks(typed)       // business rules flags

    // 6) dq_pass computation
    val checkCols = withRules.columns.filter(_.startsWith("chk_"))
    val flagged =
      if (checkCols.isEmpty) withRules.withColumn("dq_pass", lit(true))
      else withRules.withColumn("dq_pass", checkCols.map(col).reduce(_ && _))

    val passes  = flagged.filter(col("dq_pass"))
    val rejects = flagged.filter(!col("dq_pass"))

    // 7) Write bronze outputs (Parquet)
    val passOut = passes.drop(("dq_pass" +: checkCols): _*)
    writeParquetIfRows(passOut, s"$outputPath/pass")
    writeParquetIfRows(rejects, s"$outputPath/_rejects")

    // 8) Metrics
    val counts         = summarizeTotals(flagged)                               // metric, value, run_ts
    val nulls          = computeNullPercents(cleaned, tripSpec.map(_._1))      // metric, value, run_ts
    val perCheck       = summarizePerCheckMetrics(flagged)                      // metric, pass_count, fail_count, pass_rate, category, run_ts
    val domainRangeAgg = summarizeDomainRangeAggregate(flagged)                 // same as perCheck schema

    // 8a) Normalize all summaries to a unified schema so they can be unioned
    val countsU   = toUnifiedSummarySchema(counts,   defaultCategory = "counts")
    val nullsU    = toUnifiedSummarySchema(nulls,    defaultCategory = "nulls")
    val perCheckU = toUnifiedSummarySchema(perCheck, defaultCategory = "checks")
    val drU       = toUnifiedSummarySchema(domainRangeAgg, defaultCategory = "domain_range")

    val allSummary = countsU.unionByName(nullsU).unionByName(perCheckU).unionByName(drU)

    // 9) Write summaries (Parquet)
    writeParquetIfSchema(countsU,   s"$outputPath/_run_summary_counts")
    writeParquetIfSchema(nullsU,    s"$outputPath/_run_summary_nulls")
    writeParquetIfSchema(perCheckU, s"$outputPath/_run_summary_checks")
    writeParquetIfSchema(drU,       s"$outputPath/_run_summary_domain_range")
    writeParquetIfSchema(allSummary, s"$outputPath/_run_summary_all")

    println(s"Bronze trips written to: $outputPath")
    if (rejects.head(1).nonEmpty) println(s"Rejects found at: $outputPath/_rejects")

    spark.stop()
  }

  // ============================================================
  // Helpers
  // ============================================================

  // Expected schema (for type checks)
  private val tripSpec: Seq[(String, String, Boolean)] = Seq(
    ("VendorID",               "INTEGER",   true),
    ("tpep_pickup_datetime",   "TIMESTAMP", true),
    ("tpep_dropoff_datetime",  "TIMESTAMP", true),
    ("passenger_count",        "BIGINT",    true),
    ("trip_distance",          "DOUBLE",    true),
    ("RatecodeID",             "BIGINT",    true),
    ("store_and_fwd_flag",     "VARCHAR",   true),
    ("PULocationID",           "INTEGER",   true),
    ("DOLocationID",           "INTEGER",   true),
    ("payment_type",           "BIGINT",    true),
    ("fare_amount",            "DOUBLE",    true),
    ("extra",                  "DOUBLE",    true),
    ("mta_tax",                "DOUBLE",    true),
    ("tip_amount",             "DOUBLE",    true),
    ("tolls_amount",           "DOUBLE",    true),
    ("improvement_surcharge",  "DOUBLE",    true),
    ("total_amount",           "DOUBLE",    true),
    ("congestion_surcharge",   "DOUBLE",    true),
    // Input sometimes uses "Airport_fee"; include both variants downstream in range checks.
    ("airport_fee",            "DOUBLE",    true),
    ("cbd_congestion_fee",     "DOUBLE",    true)
  )

  private def toSparkType(t: String): DataType = t.toUpperCase match {
    case "INTEGER"   => IntegerType
    case "BIGINT"    => LongType
    case "DOUBLE"    => DoubleType
    case "TIMESTAMP" => TimestampType
    case "VARCHAR"   => StringType
    case _           => StringType
  }

  // Trim + empty-string-to-null for selected string columns
  private def normalizeStringCols(df: DataFrame, cols: Seq[String]): DataFrame =
    cols.foldLeft(df){ (d, c) =>
      if (d.columns.contains(c))
        d.withColumn(c,
          when(trim(col(c)) === "" || col(c).isNull, lit(null:String))
            .otherwise(trim(col(c)))
        )
      else d
    }

  // Type validation: add chk_<col>_type_ok columns
  private def applyChecks(df: DataFrame): DataFrame = {
    tripSpec.foldLeft(df) { case (tmp, (colName, typeStr, _nullable)) =>
      val chk = s"chk_${colName}_type_ok"
      if (tmp.columns.contains(colName)) {
        val dt = toSparkType(typeStr)
        tmp.withColumn(chk, col(colName).isNull || col(colName).cast(dt).isNotNull)
      } else tmp.withColumn(chk, lit(true)) // tolerate missing columns
    }
  }

  // Domain & range checks
  private val validVendors   = Seq(1, 2, 6, 7)
  private val validRateCodes = Seq(1, 2, 3, 4, 5, 6, 99)
  private val validPayTypes  = Seq(0, 1, 2, 3, 4, 5, 6)

  private def andAll(preds: Seq[Column]): Column =
    preds.reduceOption(_ && _).getOrElse(lit(true))

  private def nonNegCols(df: DataFrame, cols: Seq[String]): Column = {
    val checks = cols.filter(df.columns.contains).map(c => col(c) >= 0.0)
    andAll(checks)
  }

  private def addDomainAndRangeChecks(dfIn: DataFrame): DataFrame = {
    // Include both possible airport_fee casings
    val possibleAmtCols = Seq(
      "fare_amount","extra","mta_tax","tip_amount","tolls_amount",
      "improvement_surcharge","total_amount","congestion_surcharge",
      "airport_fee","Airport_fee","cbd_congestion_fee"
    )
    val amtCols = possibleAmtCols.filter(dfIn.columns.contains)

    dfIn
      .withColumn("chk_VendorID_domain_ok", col("VendorID").isin(validVendors:_*))
      .withColumn("chk_passenger_count_ge1", col("passenger_count") >= 1)
      .withColumn("chk_trip_distance_ge0", col("trip_distance") >= 0.0)
      .withColumn("chk_RatecodeID_domain_ok", col("RatecodeID").isin(validRateCodes:_*))
      .withColumn(
        "chk_store_and_fwd_flag_domain_ok",
        col("store_and_fwd_flag").isNull || upper(col("store_and_fwd_flag")).isin("Y","N")
      )
      .withColumn("chk_PULocationID_pos", col("PULocationID") > 0)
      .withColumn("chk_DOLocationID_pos", col("DOLocationID") > 0)
      .withColumn("chk_payment_type_domain_ok", col("payment_type").isin(validPayTypes:_*))
      .withColumn("chk_amounts_nonneg_ok", nonNegCols(dfIn, amtCols))
      .withColumn("chk_times_order_ok", col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
      .withColumn(
        "chk_duration_lt_24h",
        (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) < lit(24 * 3600)
      )
  }

  // Overall totals (metric,value,run_ts)
  private def summarizeTotals(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val checkCols = df.columns.filter(_.startsWith("chk_"))
    val total = df.count()
    val pass  = if (checkCols.isEmpty) total else df.filter(checkCols.map(col).reduce(_ && _)).count()
    val rej   = total - pass
    Seq(
      ("row_count_total",  total.toString),
      ("row_count_pass",   pass.toString),
      ("row_count_reject", rej.toString)
    ).toDF("metric","value").withColumn("run_ts", current_timestamp())
  }

  // Null % per column (metric,value,run_ts)
  private def computeNullPercents(df: DataFrame, cols: Seq[String])
                                 (implicit spark: SparkSession): DataFrame = {
    val pieces = cols.filter(df.columns.contains).map { c =>
      val dt = df.schema(c).dataType
      val isNullLike = dt match {
        case DoubleType | FloatType => col(c).isNull || isnan(col(c))
        case StringType             => col(c).isNull || length(trim(col(c))) === 0
        case _                      => col(c).isNull
      }
      val nullCount = sum(when(isNullLike, 1).otherwise(0)).cast("double")
      val total     = count(lit(1)).cast("double")
      df.agg((nullCount / total).as("v"))
        .select(lit(s"null_pct_$c").as("metric"), col("v").cast(StringType).as("value"))
        .withColumn("run_ts", current_timestamp())
    }
    pieces.reduceOption(_.unionByName(_)).getOrElse {
      import spark.implicits._
      Seq.empty[(String,String,java.sql.Timestamp)]
        .toDF("metric","value","run_ts")
    }
  }

  // Per-check metrics (metric,pass_count,fail_count,pass_rate,category,run_ts)
  private def summarizePerCheckMetrics(df: DataFrame)
                                      (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val chkCols = df.columns.filter(_.startsWith("chk_")).toSeq
    val rows = chkCols.map { c =>
      val passCount = df.filter(col(c)).count()
      val failCount = df.filter(!col(c)).count()
      val total = passCount + failCount
      val rate  = if (total == 0) 1.0 else passCount.toDouble / total.toDouble
      val cat   = if (c.endsWith("_type_ok")) "type" else "domain_range"
      (c, passCount, failCount, f"$rate%.6f", cat)
    }
    rows.toDF("metric","pass_count","fail_count","pass_rate","category")
      .withColumn("run_ts", current_timestamp())
  }

  // Aggregate metric for all domain/range checks combined
  private def summarizeDomainRangeAggregate(df: DataFrame)
                                           (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val drCols = df.columns.filter(c => c.startsWith("chk_") && !c.endsWith("_type_ok"))
    val drPass = if (drCols.isEmpty) df.count() else df.filter(drCols.map(col).reduce(_ && _)).count()
    val total  = df.count()
    val drFail = total - drPass
    val rate   = if (total == 0) 1.0 else drPass.toDouble / total.toDouble
    Seq(("domain_range_all", drPass, drFail, f"$rate%.6f", "domain_range"))
      .toDF("metric","pass_count","fail_count","pass_rate","category")
      .withColumn("run_ts", current_timestamp())
  }

  // ---------- Summary schema unification ----------
  // Target columns for all summary tables:
  // (metric, value, pass_count, fail_count, pass_rate, category, run_ts)
  private val unifiedCols: Seq[(String, DataType)] = Seq(
    "metric"     -> StringType,
    "value"      -> StringType,
    "pass_count" -> LongType,
    "fail_count" -> LongType,
    "pass_rate"  -> StringType,
    "category"   -> StringType,
    "run_ts"     -> TimestampType
  )

  private def toUnifiedSummarySchema(dfIn: DataFrame, defaultCategory: String): DataFrame = {
    // Add any missing columns as nulls of appropriate type.
    val withAll = unifiedCols.foldLeft(dfIn) { case (df, (name, dt)) =>
      if (df.columns.contains(name)) df
      else df.withColumn(name, lit(null).cast(dt))
    }
    // If category was missing, fill with provided default.
    val withCategory =
      if (withAll.columns.contains("category"))
        withAll.withColumn("category",
          when(col("category").isNull || length(trim(col("category"))) === 0, lit(defaultCategory))
            .otherwise(col("category"))
        )
      else withAll.withColumn("category", lit(defaultCategory))

    // Reorder columns to the unified order.
    withCategory.select(unifiedCols.map { case (n, _) => col(n) }:_*)
  }

  // ---------- Writers (Parquet) ----------
  private def coalesce1(df: DataFrame): DataFrame =
    if (df.rdd.partitions.size > 1) df.coalesce(1) else df

  private def writeParquetIfRows(df: DataFrame, path: String): Unit =
    if (!df.head(1).isEmpty)
      coalesce1(df).write.mode("overwrite").parquet(path)
    else println(s"Skipped Parquet write (no rows): $path")

  private def writeParquetIfSchema(df: DataFrame, path: String): Unit =
    if (df.schema.nonEmpty)
      coalesce1(df).write.mode("overwrite").parquet(path)
    else println(s"Skipped Parquet write (empty schema): $path")
}
