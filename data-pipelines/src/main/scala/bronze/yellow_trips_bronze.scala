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
 *   - Read raw Parquet from the "raw" zone.
 *   - Apply DQ validations in two layers:
 *       (1) "Type" checks using ANSI-safe try_cast() to avoid job failure.
 *       (2) "Domain/Range" checks for business rules & value sanity.
 *   - Split into pass / reject sets based on all DQ flags.
 *   - Emit pass/reject datasets and multiple summary metric tables.
 *
 * DESIGN GOALS:
 *   - Schema-tolerant: if a column is missing (schema drift), we still run
 *     and mark that particular check as TRUE (neutral) to avoid false rejects.
 *   - Fail-soft for malformed values: try_cast() returns NULL instead of
 *     throwing, so the pipeline marks a failed check rather than crashing.
 *   - Unified summary schema: all metrics normalize to one schema so they
 *     can be unioned easily and consumed by dashboards.
 *
 * OUTPUTS (under bronze_yellow_tripdata/):
 *   pass/                          valid records (without chk_* or dq_pass)
 *   _rejects/                      invalid records (with chk_* + dq_pass=false)
 *   _run_summary_counts/           (metric, value, run_ts)
 *   _run_summary_nulls/            (metric, value, run_ts)
 *   _run_summary_checks/           (metric, pass_count, fail_count, pass_rate, category, run_ts)
 *   _run_summary_domain_range/     aggregate domain/range metric (same schema as above)
 *   _run_summary_all/              union of normalized summaries for easy consumption
 *
 * OPERATIONAL NOTES:
 *   - This job is intended for local dev (master("local[*]")) or small EMR runs.
 *   - Writers coalesce(1) only for metrics in some variants; here we keep it simple
 *     and let the sink create typical partitioning files—adjust to your needs.
 *
 * Author:  Zachary Gacer
 * Date:    October 2025
 * ============================================================
 */
object YellowTripdataBronzeTripsApp {

  def main(args: Array[String]): Unit = {

    // 1) Spark session
    //    Local session for development; set log level to WARN to reduce noise.
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("BronzeApp")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // 2) I/O paths
    //    - inputPath: single monthly parquet (adjust globbing as desired).
    //    - outputPath: bronze output base dir with multiple subfolders.
    val inputPath  = "data/raw/yellow_tripdata_2025-01.parquet"
    val outputPath = "data/bronze/bronze_yellow_tripdata"

    // 3) Read raw
    //    Print schema to help spot drift quickly during local dev.
    val raw = spark.read.parquet(inputPath)
    println("Loaded input schema:")
    raw.printSchema()

    // 4) Light normalization
    //    - Typical TLC quirk: "store_and_fwd_flag" can be blank/space; null it.
    val cleaned = normalizeStringCols(raw, Seq("store_and_fwd_flag"))

    // 5) DQ checks
    //    - applyChecks: type conformance flags via try_cast (no throw).
    //    - addDomainAndRangeChecks: business rules (enums, nonnegativity, etc).
    val typed     = applyChecks(cleaned)
    val withRules = addDomainAndRangeChecks(typed)

    // 6) dq_pass computation
    //    - If there are no check columns, treat all rows as pass (defensive).
    //    - Otherwise, dq_pass = AND of all chk_* columns.
    val checkCols = withRules.columns.filter(_.startsWith("chk_"))
    val flagged =
      if (checkCols.isEmpty) withRules.withColumn("dq_pass", lit(true))
      else withRules.withColumn("dq_pass", checkCols.map(col).reduce(_ && _))

    // 7) Split pass / reject
    //    - Passes drop DQ columns; rejects retain them for inspection.
    val passes  = flagged.filter(col("dq_pass"))
    val rejects = flagged.filter(!col("dq_pass"))
    val passOut = passes.drop(("dq_pass" +: checkCols): _*)

    // 8) Write bronze outputs (Parquet)
    //    - Use "overwrite" to produce idempotent re-runs for the same month.
    writeParquetIfRows(passOut, s"$outputPath/pass")
    writeParquetIfRows(rejects, s"$outputPath/_rejects")

    // 9) Metrics
    //    - counts: total/pass/reject row counts (strings for portability).
    //    - nulls: per-column null% from the expected spec list.
    //    - perCheck: per-flag pass/fail counts and pass rate.
    //    - domainRangeAgg: aggregate across all non-type checks.
    val counts         = summarizeTotals(flagged)
    val nulls          = computeNullPercents(cleaned, tripSpec.map(_._1))
    val perCheck       = summarizePerCheckMetrics(flagged)
    val domainRangeAgg = summarizeDomainRangeAggregate(flagged)

    // 9a) Normalize summaries to a unified schema
    //     - Allows easy union + downstream read by a single schema.
    val countsU   = toUnifiedSummarySchema(counts,   defaultCategory = "counts")
    val nullsU    = toUnifiedSummarySchema(nulls,    defaultCategory = "nulls")
    val perCheckU = toUnifiedSummarySchema(perCheck, defaultCategory = "checks")
    val drU       = toUnifiedSummarySchema(domainRangeAgg, defaultCategory = "domain_range")

    val allSummary = countsU.unionByName(nullsU).unionByName(perCheckU).unionByName(drU)

    // 10) Write summary tables (Parquet)
    writeParquetIfSchema(countsU,   s"$outputPath/_run_summary_counts")
    writeParquetIfSchema(nullsU,    s"$outputPath/_run_summary_nulls")
    writeParquetIfSchema(perCheckU, s"$outputPath/_run_summary_checks")
    writeParquetIfSchema(drU,       s"$outputPath/_run_summary_domain_range")
    writeParquetIfSchema(allSummary, s"$outputPath/_run_summary_all")

    // 11) Friendly logs to find outputs quickly
    println(s"Bronze trips written to: $outputPath")
    if (rejects.head(1).nonEmpty) println(s"Rejects found at: $outputPath/_rejects")

    // 12) Done
    spark.stop()
  }

  // ============================================================
  // Helpers: schema spec and common utilities
  // ============================================================

  /**
   * tripSpec
   * --------
   * Expected columns with "portable" SQL type names and (currently unused) nullability.
   * - We keep types generic (INTEGER/BIGINT/DOUBLE/TIMESTAMP/VARCHAR) and map them
   *   to Spark types when needed. This helps reuse the same definition for try_cast strings.
   * - If the raw dataset introduces new columns, they simply pass through and are ignored
   *   by DQ unless added here.
   */
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
    // Some months use "Airport_fee" instead of "airport_fee" — both handled later.
    ("airport_fee",            "DOUBLE",    true),
    ("cbd_congestion_fee",     "DOUBLE",    true)
  )

  /** Map our portable type names to Spark DataTypes (for non-SQL expr use). */
  private[bronze] def toSparkType(t: String): DataType = t.toUpperCase match {
    case "INTEGER"   => IntegerType
    case "BIGINT"    => LongType
    case "DOUBLE"    => DoubleType
    case "TIMESTAMP" => TimestampType
    case "VARCHAR"   => StringType
    case _           => StringType
  }

  /** Map portable type names to SQL type strings usable inside expr("try_cast(... as TYPE)") */
  private def toSqlTypeName(t: String): String = t.toUpperCase match {
    case "INTEGER"   => "INT"
    case "BIGINT"    => "BIGINT"
    case "DOUBLE"    => "DOUBLE"
    case "TIMESTAMP" => "TIMESTAMP"
    case "VARCHAR"   => "STRING"
    case other       => other // Fallback: use as-is
  }

  /**
   * normalizeStringCols
   * -------------------
   * Trim selected string columns and convert empty strings to NULL.
   * Useful for legacy fields that may contain blanks (e.g., " ", "").
   */
  private[bronze] def normalizeStringCols(df: DataFrame, cols: Seq[String]): DataFrame =
    cols.foldLeft(df){ (d, c) =>
      if (d.columns.contains(c))
        d.withColumn(
          c,
          when(trim(col(c)) === "" || col(c).isNull, lit(null:String)) // normalize blank -> NULL
            .otherwise(trim(col(c)))
        )
      else d // schema-tolerant: skip if column missing
    }

  /**
   * applyChecks
   * -----------
   * For each expected column, add a boolean flag "chk_<col>_type_ok".
   * Logic:
   *   - If column exists:
   *       chk = (col IS NULL) OR (try_cast(col as expected_type) IS NOT NULL)
   *     This treats NULL as "not a type error", and malformed strings as failed type checks.
   *   - If column missing:
   *       chk = true  (schema tolerance: absence doesn't fail the row)
   *
   * Why try_cast?
   *   Spark 4’s ANSI mode throws on invalid cast (e.g., "X" -> INT).
   *   try_cast returns NULL, allowing us to mark a failed check instead of crashing.
   */
  private[bronze] def applyChecks(df: DataFrame): DataFrame = {
    tripSpec.foldLeft(df) { case (tmp, (colName, typeStr, _nullable)) =>
      val chk = s"chk_${colName}_type_ok"
      if (tmp.columns.contains(colName)) {
        val sqlT = toSqlTypeName(typeStr)
        val ok   = col(colName).isNull || expr(s"try_cast(`${colName}` as $sqlT)").isNotNull
        tmp.withColumn(chk, ok)
      } else {
        tmp.withColumn(chk, lit(true)) // neutral if missing
      }
    }
  }

  // Enumerations from TLC spec (keep in one place for easy maintenance)
  private val validVendors   = Seq(1, 2, 6, 7)
  private val validRateCodes = Seq(1, 2, 3, 4, 5, 6, 99)
  private val validPayTypes  = Seq(0, 1, 2, 3, 4, 5, 6)

  /** Safe AND across a sequence of boolean Columns (TRUE if empty). */
  private def andAll(preds: Seq[Column]): Column =
    preds.reduceOption(_ && _).getOrElse(lit(true))

  /**
   * nonNegCols
   * ----------
   * Build a single Column asserting that all provided numeric columns are >= 0.
   * Missing columns are ignored (schema tolerant).
   */
  private def nonNegCols(df: DataFrame, cols: Seq[String]): Column = {
    val checks = cols.filter(df.columns.contains).map(c => col(c) >= 0.0)
    andAll(checks)
  }

  /** Convenience to check column presence repeatedly. */
  private def has(df: DataFrame, c: String): Boolean = df.columns.contains(c)

  /**
   * addDomainAndRangeChecks
   * -----------------------
   * Business-rule validations (schema tolerant):
   *   - Enumerations: VendorID, RatecodeID, payment_type.
   *   - Nonnegativity: fare/extra/tips/tolls/etc.
   *   - Time sanity: dropoff >= pickup; duration < 24h (heuristic).
   *   - Flags: store_and_fwd_flag ∈ {Y,N} (case-insensitive); allow NULL.
   *   - Location IDs > 0 if present.
   *
   * Strategy:
   *   - For each check, if the referenced column(s) are missing, produce TRUE
   *     (neutral) to avoid penalizing rows due to schema differences.
   */
  private[bronze] def addDomainAndRangeChecks(dfIn: DataFrame): DataFrame = {
    // Consider both "airport_fee" and "Airport_fee" variants
    val possibleAmtCols = Seq(
      "fare_amount","extra","mta_tax","tip_amount","tolls_amount",
      "improvement_surcharge","total_amount","congestion_surcharge",
      "airport_fee","Airport_fee","cbd_congestion_fee"
    )
    val amtCols = possibleAmtCols.filter(dfIn.columns.contains)

    val withVendor =
      if (has(dfIn, "VendorID"))
        dfIn.withColumn("chk_VendorID_domain_ok", col("VendorID").isin(validVendors:_*))
      else dfIn.withColumn("chk_VendorID_domain_ok", lit(true))

    val withPax =
      if (has(withVendor, "passenger_count"))
        withVendor.withColumn("chk_passenger_count_ge1", col("passenger_count") >= 1)
      else withVendor.withColumn("chk_passenger_count_ge1", lit(true))

    val withDist =
      if (has(withPax, "trip_distance"))
        withPax.withColumn("chk_trip_distance_ge0", col("trip_distance") >= 0.0)
      else withPax.withColumn("chk_trip_distance_ge0", lit(true))

    val withRate =
      if (has(withDist, "RatecodeID"))
        withDist.withColumn("chk_RatecodeID_domain_ok", col("RatecodeID").isin(validRateCodes:_*))
      else withDist.withColumn("chk_RatecodeID_domain_ok", lit(true))

    val withSfwd =
      if (has(withRate, "store_and_fwd_flag"))
        withRate.withColumn(
          "chk_store_and_fwd_flag_domain_ok",
          col("store_and_fwd_flag").isNull || upper(col("store_and_fwd_flag")).isin("Y","N")
        )
      else withRate.withColumn("chk_store_and_fwd_flag_domain_ok", lit(true))

    val withPU =
      if (has(withSfwd, "PULocationID"))
        withSfwd.withColumn("chk_PULocationID_pos", col("PULocationID") > 0)
      else withSfwd.withColumn("chk_PULocationID_pos", lit(true))

    val withDO =
      if (has(withPU, "DOLocationID"))
        withPU.withColumn("chk_DOLocationID_pos", col("DOLocationID") > 0)
      else withPU.withColumn("chk_DOLocationID_pos", lit(true))

    val withPay =
      if (has(withDO, "payment_type"))
        withDO.withColumn("chk_payment_type_domain_ok", col("payment_type").isin(validPayTypes:_*))
      else withDO.withColumn("chk_payment_type_domain_ok", lit(true))

    val withAmts =
      withPay.withColumn("chk_amounts_nonneg_ok", nonNegCols(withPay, amtCols))

    val withTimesOrdered =
      if (has(withAmts, "tpep_pickup_datetime") && has(withAmts, "tpep_dropoff_datetime"))
        withAmts.withColumn("chk_times_order_ok", col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
      else withAmts.withColumn("chk_times_order_ok", lit(true))

    val withDuration =
      if (has(withTimesOrdered, "tpep_pickup_datetime") && has(withTimesOrdered, "tpep_dropoff_datetime"))
        withTimesOrdered.withColumn(
          "chk_duration_lt_24h",
          (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) < lit(24 * 3600)
        )
      else withTimesOrdered.withColumn("chk_duration_lt_24h", lit(true))

    withDuration
  }

  // ============================================================
  // Metrics: counts, null%, per-check, aggregate
  // ============================================================

  /**
   * summarizeTotals
   * ---------------
   * Produce simple row counts:
   *   - row_count_total
   *   - row_count_pass  (AND of all chk_* flags)
   *   - row_count_reject
   * Stored as (metric, value, run_ts) where value is String for portability.
   */
  private[bronze] def summarizeTotals(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
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

  /**
   * computeNullPercents
   * -------------------
   * For each provided column name (if present in df), compute NULL percentage.
   *   - Numeric (float/double): treat NaN as NULL-like.
   *   - String: trim and treat empty string as NULL-like.
   * Output schema: (metric="null_pct_<col>", value=StringFraction, run_ts)
   */
  private[bronze] def computeNullPercents(df: DataFrame, cols: Seq[String])
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
    // If none of the columns exist, return an empty, correctly-typed frame.
    pieces.reduceOption(_.unionByName(_)).getOrElse {
      import spark.implicits._
      Seq.empty[(String,String,java.sql.Timestamp)]
        .toDF("metric","value","run_ts")
    }
  }

  /**
   * summarizePerCheckMetrics
   * ------------------------
   * For each chk_* flag present:
   *   - pass_count = count(chk = TRUE)
   *   - fail_count = count(chk = FALSE)
   *   - pass_rate  = pass_count / (pass_count + fail_count)
   * Category is derived:
   *   - endsWith("_type_ok") -> "type"
   *   - else                 -> "domain_range"
   */
  private[bronze] def summarizePerCheckMetrics(df: DataFrame)
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

  /**
   * summarizeDomainRangeAggregate
   * -----------------------------
   * Aggregate a single metric across all non-type checks:
   *   - metric = "domain_range_all"
   *   - pass_count = #rows where ALL domain/range checks are TRUE
   *   - fail_count = total - pass_count
   *   - pass_rate = pass_count / total
   */
  private[bronze] def summarizeDomainRangeAggregate(df: DataFrame)
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

  // ============================================================
  // Summary schema unification & writers
  // ============================================================

  /**
   * unifiedCols
   * -----------
   * Canonical metric schema so that different summary frames can be unioned
   * without per-consumer branching logic.
   */
  private val unifiedCols: Seq[(String, DataType)] = Seq(
    "metric"     -> StringType,
    "value"      -> StringType,
    "pass_count" -> LongType,
    "fail_count" -> LongType,
    "pass_rate"  -> StringType,
    "category"   -> StringType,
    "run_ts"     -> TimestampType
  )

  /**
   * toUnifiedSummarySchema
   * ----------------------
   * Normalize a summary DataFrame to the canonical schema above:
   *   - Add any missing columns with NULLs of the right type.
   *   - Fill or override "category" with defaultCategory if missing/blank.
   *   - Reorder columns to canonical order.
   */
  private[bronze] def toUnifiedSummarySchema(dfIn: DataFrame, defaultCategory: String): DataFrame = {
    // Add any missing columns as nulls of appropriate type.
    val withAll = unifiedCols.foldLeft(dfIn) { case (df, (name, dt)) =>
      if (df.columns.contains(name)) df
      else df.withColumn(name, lit(null).cast(dt))
    }
    // Fill category if missing/blank.
    val withCategory =
      if (withAll.columns.contains("category"))
        withAll.withColumn(
          "category",
          when(col("category").isNull || length(trim(col("category"))) === 0, lit(defaultCategory))
            .otherwise(col("category"))
        )
      else withAll.withColumn("category", lit(defaultCategory))

    // Reorder columns to the unified order for deterministic consumers.
    withCategory.select(unifiedCols.map { case (n, _) => col(n) }:_*)
  }

  // ---------- Writers (Parquet) ----------

  /** coalesce1: reduce partitions to a single file if >1 (used sparingly). */
  private[bronze] def coalesce1(df: DataFrame): DataFrame =
    if (df.rdd.partitions.size > 1) df.coalesce(1) else df

  /**
   * writeParquetIfRows
   * ------------------
   * Write if there is at least one row; otherwise log and skip.
   * Overwrite mode supports idempotent re-runs for a given input slice.
   */
  private[bronze] def writeParquetIfRows(df: DataFrame, path: String): Unit =
    if (!df.head(1).isEmpty)
      coalesce1(df).write.mode("overwrite").parquet(path)
    else println(s"Skipped Parquet write (no rows): $path")

  /**
   * writeParquetIfSchema
   * --------------------
   * Write if schema is non-empty (some metric frames could be empty).
   * This avoids creating empty directories without a schema.
   */
  private[bronze] def writeParquetIfSchema(df: DataFrame, path: String): Unit =
    if (df.schema.nonEmpty)
      coalesce1(df).write.mode("overwrite").parquet(path)
    else println(s"Skipped Parquet write (empty schema): $path")
} 
