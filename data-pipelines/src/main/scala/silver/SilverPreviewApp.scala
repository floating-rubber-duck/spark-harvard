package silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * ============================================================
 * SilverPreviewApp
 * ------------------------------------------------------------
 * Small utility to inspect Silver-layer outputs without needing
 * the standalone spark-shell binary. Invoked via:
 *
 *   sbt "run --job show_silver"
 *
 * Environment/system props:
 *   -DsilverPath=<path>     (default: data/silver/silver_yellow_tripdata)
 *   -DsampleSize=<rows>     (default: 20)
 * ============================================================
 */
object SilverPreviewApp {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("SilverPreview")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val basePath   = sys.props.getOrElse("silverPath", "data/silver/silver_yellow_tripdata")
    val sampleSize = sys.props.get("sampleSize").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(20)

    val curatedPath = s"$basePath/curated"
    val weeklyPath  = s"$basePath/_run_summary_weekly"

    println(s"Reading curated dataset from: $curatedPath")
    val curatedDf = spark.read.parquet(curatedPath)

    println(s"\n=== Curated Columns (${curatedDf.columns.length}) ===")
    curatedDf.printSchema()

    println(s"\n=== Sample ${sampleSize} Rows ===")
    curatedDf.orderBy(col("pickup_datetime")).show(sampleSize, truncate = false)

    println(s"\n=== KPI Metrics Preview ===")
    curatedDf.agg(
      count(lit(1)).as("row_count"),
      sum("total_amount").as("total_revenue"),
      sum("trip_distance").as("total_miles"),
      avg("trip_distance").as("avg_trip_distance"),
      avg("trip_minutes").as("avg_trip_minutes"),
      avg(expr("IF(is_peak_hour, 1, 0)")).as("peak_hour_pct"),
      avg(expr("IF(is_night, 1, 0)")).as("night_trip_pct")
    ).show(truncate = false)

    println(s"\nReading weekly summary from: $weeklyPath")
    val weeklyDf = spark.read.parquet(weeklyPath)
    weeklyDf.orderBy(col("pickup_week_start")).show(truncate = false)

    spark.stop()
  }
}
