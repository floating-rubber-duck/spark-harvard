package silver.kpis

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * YellowTripsGold
 *
 * Reads Silver (curated) parquet output produced by YellowTripdataSilverApp
 * and computes weekly KPI aggregates by pickup_week_id & pu_borough.
 *
 * Expects Silver columns (from your Silver code):
 *  - pickup_week_id: String (e.g. "2025-W01")
 *  - pickup_week_start: Date
 *  - pu_borough: String
 *  - pickup_datetime, pickup_hour: Timestamp/Int
 *  - trip_minutes: Double (nullable)
 *  - trip_distance: Double
 *  - total_amount: Double
 *
 * Outputs:
 *  - gold parquet at goldOutputPath (partitioned by pickup_week_id)
 *  - previews CSVs at goldPreviewPath (small CSV snapshots per KPI)
 *  - run_summary JSON (single-row) at goldPreviewPath/run_summary
 */
object YellowTripsGold {

  case class Conf(
    silverCuratedPath: String,
    goldOutputPath: String,
    goldPreviewPath: String,
    runId: String,
    minRowsForWeekWarning: Long
  )

  def loadConf(): Conf = {
    val now = LocalDateTime.now()
    val runId = now.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    Conf(
      silverCuratedPath = sys.props.getOrElse("silverCuratedPath", "data/silver/silver_yellow_tripdata/curated"),
      goldOutputPath    = sys.props.getOrElse("goldOutputPath", "data/gold/gold_yellow_tripdata"),
      goldPreviewPath   = sys.props.getOrElse("goldPreviewPath", "data/gold_preview/gold_yellow_tripdata"),
      runId             = sys.props.getOrElse("runId", runId),
      minRowsForWeekWarning = sys.props.get("minRowsForWeekWarning").map(_.toLong).getOrElse(50L)
    )
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("YellowTripsGold")
      .master(sys.props.getOrElse("sparkMaster", "local[*]"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val conf = loadConf()

    val silver = spark.read.parquet(conf.silverCuratedPath)
    if (silver.limit(1).count() == 0) {
      println(s"No rows found in silver path: ${conf.silverCuratedPath}; exiting.")
      spark.stop()
      return
    }

    val kpis = computeWeeklyKPIs(silver)
    writeGold(kpis, conf.goldOutputPath)
    writePreviewsAndSummary(kpis, conf.goldPreviewPath, conf)

    println(s"Gold write complete -> ${conf.goldOutputPath}")
    println(s"Preview CSVs + run summary -> ${conf.goldPreviewPath}")
    spark.stop()
  }

  /** Compute KPIs grouped by pickup_week_id and pu_borough */
  def computeWeeklyKPIs(silver: DataFrame): DataFrame = {
    val df = silver

    // Ensure required columns exist (throw helpful message if missing)
    val required = Seq(
      "pickup_week_id", "pickup_week_start", "pu_borough",
      "pickup_datetime", "trip_minutes", "trip_distance", "total_amount"
    )
    required.foreach { c =>
      if (!df.columns.contains(c)) {
        throw new IllegalStateException(s"Silver dataset missing required column: $c")
      }
    }

    // Ensure pickup_hour exists or derive it
    val withHour = if (df.columns.contains("pickup_hour")) df else df.withColumn("pickup_hour", hour(col("pickup_datetime")))

    // Safe borough: replace null/empty with "UNKNOWN"
    val safeBorough = withHour.withColumn("pu_borough", when(col("pu_borough").isNull || trim(col("pu_borough")) === "", lit("UNKNOWN")).otherwise(col("pu_borough")))

    // Use Silver flags if present, otherwise derive from hour
    val withFlags =
      if (safeBorough.columns.contains("is_peak_hour") && safeBorough.columns.contains("is_night")) {
        safeBorough
          .withColumn("is_peak", col("is_peak_hour") === true)
          .withColumn("is_night_flag", col("is_night") === true)
      } else {
        safeBorough
          .withColumn("is_peak", col("pickup_hour").isin(7, 8, 17, 18))
          .withColumn("is_night_flag", col("pickup_hour").between(22, 23) || col("pickup_hour").between(0, 5))
      }

    // Aggregation
    val agg = withFlags.groupBy("pickup_week_id", "pickup_week_start", "pu_borough")
      .agg(
        count(lit(1)).as("weekly_trips"),
        sum(col("total_amount")).cast(DoubleType).as("weekly_revenue"),
        sum(col("trip_distance")).cast(DoubleType).as("sum_trip_distance"),
        avg(col("trip_minutes")).as("avg_trip_time_minutes"),
        avg(col("trip_distance")).as("avg_trip_distance_miles"),
        sum(when(col("is_peak"), 1).otherwise(0)).as("peak_hour_trips"),
        sum(when(col("is_night_flag"), 1).otherwise(0)).as("night_trips")
      )
      .withColumn("peak_hour_pct", when(col("weekly_trips") === 0, lit(0.0))
        .otherwise((col("peak_hour_trips") / col("weekly_trips")).cast(DoubleType)))
      .withColumn("night_trip_pct", when(col("weekly_trips") === 0, lit(0.0))
        .otherwise((col("night_trips") / col("weekly_trips")).cast(DoubleType)))
      .withColumn("avg_revenue_per_mile",
        when(col("sum_trip_distance").isNull || col("sum_trip_distance") === 0.0, lit(null).cast(DoubleType))
          .otherwise((col("weekly_revenue") / col("sum_trip_distance")).cast(DoubleType))
      )
      .select(
        col("pickup_week_id"),
        col("pickup_week_start"),
        col("pu_borough"),
        col("weekly_trips"),
        col("weekly_revenue"),
        col("sum_trip_distance"),
        col("avg_trip_distance_miles"),
        col("avg_trip_time_minutes"),
        col("peak_hour_trips"),
        col("peak_hour_pct"),
        col("night_trips"),
        col("night_trip_pct"),
        col("avg_revenue_per_mile")
      )
      .orderBy(col("pickup_week_start").asc, col("pu_borough").asc)

    agg
  }

  /** Write gold parquet partitioned by pickup_week_id */
  def writeGold(kpiDf: DataFrame, goldPath: String): Unit = {
    kpiDf.write.mode(SaveMode.Overwrite).partitionBy("pickup_week_id").parquet(goldPath)
  }

  /** Write several small preview CSVs and a run summary JSON */
  def writePreviewsAndSummary(kpiDf: DataFrame, previewBase: String, conf: Conf): Unit = {
    val spark = kpiDf.sparkSession
    import spark.implicits._

    // Ensure preview dir exists by coalescing small writes
    val coalesced = if (kpiDf.rdd.partitions.length > 1) kpiDf.coalesce(1) else kpiDf

    // 1) Full KPI snapshot (coalesced) - limited rows for preview
    val snapshot = coalesced.limit(500)
    snapshot.write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$previewBase/full_kpis")

    // 2) Per-KPI CSVs (sliced views)
    val tripsByWeek = kpiDf.select("pickup_week_id", "pu_borough", "weekly_trips")
      .orderBy("pickup_week_id", "pu_borough").limit(500)
    tripsByWeek.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$previewBase/weekly_trips")

    val revenueByWeek = kpiDf.select("pickup_week_id", "pu_borough", "weekly_revenue")
      .orderBy("pickup_week_id", "pu_borough").limit(500)
    revenueByWeek.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$previewBase/weekly_revenue")

    val peakPct = kpiDf.select("pickup_week_id", "pu_borough", "peak_hour_pct")
      .orderBy("pickup_week_id", "pu_borough").limit(500)
    peakPct.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$previewBase/peak_hour_pct")

    val nightPct = kpiDf.select("pickup_week_id", "pu_borough", "night_trip_pct")
      .orderBy("pickup_week_id", "pu_borough").limit(500)
    nightPct.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$previewBase/night_trip_pct")

    val avgRevPerMile = kpiDf.select("pickup_week_id", "pu_borough", "avg_revenue_per_mile")
      .orderBy("pickup_week_id", "pu_borough").limit(500)
    avgRevPerMile.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$previewBase/avg_revenue_per_mile")

    // 3) Run summary small JSON (single-line). Compute some run metrics:
    val totalRowsRow = kpiDf.agg(sum(col("weekly_trips")).cast(LongType).as("total_weekly_trips")).first()
    val totalRows = if (totalRowsRow == null) 0L else totalRowsRow.getAs[Long]("total_weekly_trips")
    val weekCount = kpiDf.select("pickup_week_id").distinct().count()

    val runSummary = Seq((
      conf.runId,
      java.time.Instant.now().toString,
      conf.silverCuratedPath,
      conf.goldOutputPath,
      weekCount,
      totalRows
    )).toDF("run_id","run_ts","silver_path","gold_path","weeks_covered","total_weekly_trips")

    runSummary.coalesce(1).write.mode(SaveMode.Overwrite).json(s"$previewBase/run_summary")
  }
}
