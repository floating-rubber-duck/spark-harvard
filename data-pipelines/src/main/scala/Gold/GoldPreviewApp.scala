package silver.kpis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * ============================================================
 * GoldPreviewApp
 * ------------------------------------------------------------
 * Purpose:
 *   Simple utility to explore Gold-layer KPI outputs and
 *   produce quick summaries for validation or screenshots.
 *
 * Run with:
 *   sbt "runMain silver.kpis.GoldPreviewApp"
 *
 * System properties (optional):
 *   -DgoldPath=<path>         (default: data/gold/gold_yellow_tripdata)
 *   -DgoldPreviewPath=<path>  (default: data/gold_preview/gold_yellow_tripdata)
 *   -DsampleSize=<rows>       (default: 20)
 * ============================================================
 */
object GoldPreviewApp {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("GoldPreviewApp")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val goldPath       = sys.props.getOrElse("goldPath", "data/gold/gold_yellow_tripdata")
    val goldPreviewDir = sys.props.getOrElse("goldPreviewPath", "data/gold_preview/gold_yellow_tripdata")
    val sampleSize     = sys.props.get("sampleSize").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(20)

    println(s"Reading Gold KPIs from: $goldPath")
    val goldDf = spark.read.parquet(goldPath)

    println(s"\n=== Gold Schema (${goldDf.columns.length} columns) ===")
    goldDf.printSchema()

    println(s"\n=== Sample $sampleSize KPI Rows ===")
    goldDf.orderBy(col("pickup_week_start"), col("pu_borough"))
      .show(sampleSize, truncate = false)

    println(s"\n=== KPI Summary ===")
    goldDf.agg(
      count(lit(1)).as("num_week_borough_combinations"),
      sum("weekly_trips").as("total_trips"),
      sum("weekly_revenue").as("total_revenue"),
      avg("avg_trip_distance_miles").as("mean_avg_distance_miles"),
      avg("avg_trip_time_minutes").as("mean_avg_time_minutes"),
      avg("peak_hour_pct").as("mean_peak_hour_pct"),
      avg("night_trip_pct").as("mean_night_trip_pct"),
      avg("avg_revenue_per_mile").as("mean_revenue_per_mile")
    ).show(truncate = false)

    println(s"\n=== Top Boroughs by Weekly Trips ===")
    goldDf.orderBy(desc("weekly_trips"))
      .select("pickup_week_id", "pu_borough", "weekly_trips", "weekly_revenue")
      .show(10, truncate = false)

    println(s"\nReading preview CSVs (if available) from: $goldPreviewDir")
    try {
      val samplePreview = spark.read.option("header", "true").csv(s"$goldPreviewDir/weekly_trips")
      samplePreview.show(10, truncate = false)
    } catch {
      case _: Throwable =>
        println("⚠️  No preview CSVs found — run YellowTripsGold first to generate them.")
    }

    spark.stop()
  }
}
