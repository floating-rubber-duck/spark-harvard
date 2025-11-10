package silver

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * ============================================================
 * YellowTripdataSilverApp
 * ------------------------------------------------------------
 * Purpose:
 *   Consume Bronze-layer pass datasets, conform schema, enrich
 *   with taxi zone dimension data, and surface derived features
 *   required for Gold (KPI) aggregations.
 *
 * Outputs (under data/silver/silver_yellow_tripdata/):
 *   curated/                 → clean, conformed trip facts
 *   _run_summary_weekly/     → row counts + revenue totals per pickup week
 *
 * Author: Codex teammate, Oct 2025
 * ============================================================
 */
object YellowTripdataSilverApp {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("SilverApp")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val bronzeTripsPath =
      sys.props.getOrElse("bronzeTripsPath", "data/bronze/bronze_yellow_tripdata/pass")
    val bronzeZonesPath =
      sys.props.getOrElse("bronzeZonesPath", "data/bronze/bronze_taxi_zone/full_dq_checked")
    val silverOutputPath =
      sys.props.getOrElse("silverOutputPath", "data/silver/silver_yellow_tripdata")

    val bronzeTrips = spark.read.parquet(bronzeTripsPath)
    val bronzeZones = spark.read.parquet(bronzeZonesPath)

    val standardized = YellowTripdataSilverTransforms.standardizeTrips(bronzeTrips)
    val withZones    = YellowTripdataSilverTransforms.joinTaxiZones(standardized, bronzeZones)
    val enriched     = YellowTripdataSilverTransforms.addDerivedColumns(withZones)
    val finalDf      = YellowTripdataSilverTransforms.finalizeDataset(enriched)

    YellowTripdataSilverTransforms.writeOutputs(finalDf, silverOutputPath)

    println(s"Silver trips written to: $silverOutputPath")
    spark.stop()
  }
}

/** Helpers for the silver-layer transformations. */
object YellowTripdataSilverTransforms {

  private val expectedTripTypes: Seq[(String, DataType)] = Seq(
    "vendor_id"            -> IntegerType,
    "pickup_datetime"      -> TimestampType,
    "dropoff_datetime"     -> TimestampType,
    "passenger_count"      -> IntegerType,
    "trip_distance"        -> DoubleType,
    "rate_code_id"         -> IntegerType,
    "store_and_fwd_flag"   -> StringType,
    "pu_location_id"       -> IntegerType,
    "do_location_id"       -> IntegerType,
    "payment_type"         -> IntegerType,
    "fare_amount"          -> DoubleType,
    "extra"                -> DoubleType,
    "mta_tax"              -> DoubleType,
    "tip_amount"           -> DoubleType,
    "tolls_amount"         -> DoubleType,
    "improvement_surcharge"-> DoubleType,
    "total_amount"         -> DoubleType,
    "congestion_surcharge" -> DoubleType,
    "airport_fee"          -> DoubleType,
    "cbd_congestion_fee"   -> DoubleType
  )

  private val bronzeToSilverRenames: Map[String, String] = Map(
    "VendorID"              -> "vendor_id",
    "tpep_pickup_datetime"  -> "pickup_datetime",
    "tpep_dropoff_datetime" -> "dropoff_datetime",
    "passenger_count"       -> "passenger_count",
    "trip_distance"         -> "trip_distance",
    "RatecodeID"            -> "rate_code_id",
    "store_and_fwd_flag"    -> "store_and_fwd_flag",
    "PULocationID"          -> "pu_location_id",
    "DOLocationID"          -> "do_location_id",
    "payment_type"          -> "payment_type",
    "fare_amount"           -> "fare_amount",
    "extra"                 -> "extra",
    "mta_tax"               -> "mta_tax",
    "tip_amount"            -> "tip_amount",
    "tolls_amount"          -> "tolls_amount",
    "improvement_surcharge" -> "improvement_surcharge",
    "total_amount"          -> "total_amount",
    "congestion_surcharge"  -> "congestion_surcharge",
    "airport_fee"           -> "airport_fee",
    "Airport_fee"           -> "airport_fee",
    "cbd_congestion_fee"    -> "cbd_congestion_fee"
  )

  /** Ensure bronze trips conform to the curated silver schema. */
  def standardizeTrips(dfIn: DataFrame): DataFrame = {
    val normalizedAirport =
      if (dfIn.columns.contains("Airport_fee") && !dfIn.columns.contains("airport_fee"))
        dfIn.withColumn("airport_fee", col("Airport_fee"))
      else dfIn

    val renamed = bronzeToSilverRenames.foldLeft(normalizedAirport) { case (tmp, (src, target)) =>
      if (tmp.columns.contains(src) && src != target) tmp.withColumnRenamed(src, target) else tmp
    }

    val withAllColumns = expectedTripTypes.foldLeft(renamed) { case (tmp, (name, dt)) =>
      if (tmp.columns.contains(name)) tmp
      else tmp.withColumn(name, lit(null).cast(dt))
    }

    expectedTripTypes.foldLeft(withAllColumns) { case (tmp, (name, dt)) =>
      val casted = tmp.withColumn(name, col(name).cast(dt))
      if (name == "store_and_fwd_flag")
        casted.withColumn(name,
          when(col(name).isNull, lit(null:String))
            .otherwise(upper(trim(col(name)))))
      else casted
    }
  }

  /** Clean taxi-zone lookup and join onto pickup/dropoff IDs. */
  def joinTaxiZones(trips: DataFrame, zonesIn: DataFrame): DataFrame = {
    val zones = zonesIn
      .withColumn("location_id", col("LocationID").cast(IntegerType))
      .withColumn("borough", trim(col("Borough")))
      .withColumn("zone", trim(col("Zone")))
      .withColumn("service_zone", trim(col("service_zone")))
      .filter(col("location_id").isNotNull)
      .dropDuplicates("location_id")

    val puZones = zones.select(
      col("location_id").as("pu_location_id"),
      col("borough").as("pu_borough"),
      col("zone").as("pu_zone"),
      col("service_zone").as("pu_service_zone")
    )

    val doZones = zones.select(
      col("location_id").as("do_location_id"),
      col("borough").as("do_borough"),
      col("zone").as("do_zone"),
      col("service_zone").as("do_service_zone")
    )

    val withPu = trips.join(puZones, Seq("pu_location_id"), "left")
    withPu.join(doZones, Seq("do_location_id"), "left")
  }

  /** Add temporal features and KPI-friendly derived metrics. */
  def addDerivedColumns(dfIn: DataFrame): DataFrame = {
    val base = dfIn
      .withColumn("pickup_datetime", col("pickup_datetime").cast(TimestampType))
      .withColumn("dropoff_datetime", col("dropoff_datetime").cast(TimestampType))
      .withColumn("pickup_date", to_date(col("pickup_datetime")))
      .withColumn("pickup_year", year(col("pickup_datetime")))
      .withColumn("pickup_week_of_year", weekofyear(col("pickup_datetime")))
      .withColumn("pickup_week_start", date_trunc("week", col("pickup_datetime")).cast(DateType))
      .withColumn(
        "pickup_week_id",
        format_string("%04d-W%02d", col("pickup_year"), col("pickup_week_of_year"))
      )
      .withColumn("pickup_hour", hour(col("pickup_datetime")))
      .withColumn("pickup_day_of_week", date_format(col("pickup_datetime"), "E"))
      .withColumn("trip_minutes",
        ((col("dropoff_datetime").cast(LongType) - col("pickup_datetime").cast(LongType)) / 60.0)
          .cast(DoubleType))
      .withColumn("trip_minutes",
        when(col("trip_minutes") < 0, lit(null).cast(DoubleType)).otherwise(col("trip_minutes")))
      .withColumn("trip_hours", col("trip_minutes") / 60.0)

    val withFlags = base
      .withColumn(
        "is_peak_hour",
        col("pickup_hour").between(7, 8) || col("pickup_hour").between(17, 18)
      )
      .withColumn(
        "is_night",
        col("pickup_hour") >= 20 || col("pickup_hour") < 6
      )
      .withColumn(
        "is_weekend",
        dayofweek(col("pickup_datetime")).isin(1, 7)
      )

    withFlags
      .withColumn("revenue_per_mile",
        when(col("trip_distance") > 0, col("total_amount") / col("trip_distance"))
          .otherwise(lit(null).cast(DoubleType)))
      .withColumn("revenue_per_minute",
        when(col("trip_minutes") > 0, col("total_amount") / col("trip_minutes"))
          .otherwise(lit(null).cast(DoubleType)))
      .withColumn("trip_mph",
        when(col("trip_minutes") > 0, col("trip_distance") / (col("trip_minutes") / 60.0))
          .otherwise(lit(null).cast(DoubleType)))
  }

  /** Deduplicate, surface ride_id, and enforce column order. */
  def finalizeDataset(dfIn: DataFrame): DataFrame = {
    val withRideId = dfIn.withColumn(
      "ride_id",
      sha2(concat_ws("::",
        coalesce(col("vendor_id").cast(StringType), lit("")),
        date_format(col("pickup_datetime"), "yyyy-MM-dd'T'HH:mm:ss"),
        coalesce(col("pickup_week_id"), lit("")),
        coalesce(col("pu_location_id").cast(StringType), lit("")),
        coalesce(col("do_location_id").cast(StringType), lit("")),
        coalesce(col("total_amount").cast(StringType), lit(""))
      ), 256)
    )

    val deduped = withRideId.dropDuplicates("ride_id")

    val orderedCols: Seq[String] = Seq(
      "ride_id",
      "vendor_id",
      "pickup_datetime",
      "dropoff_datetime",
      "pickup_date",
      "pickup_hour",
      "pickup_day_of_week",
      "pickup_year",
      "pickup_week_of_year",
      "pickup_week_start",
      "pickup_week_id",
      "passenger_count",
      "trip_distance",
      "trip_minutes",
      "trip_hours",
      "rate_code_id",
      "store_and_fwd_flag",
      "pu_location_id",
      "pu_borough",
      "pu_zone",
      "pu_service_zone",
      "do_location_id",
      "do_borough",
      "do_zone",
      "do_service_zone",
      "payment_type",
      "fare_amount",
      "extra",
      "mta_tax",
      "tip_amount",
      "tolls_amount",
      "improvement_surcharge",
      "total_amount",
      "congestion_surcharge",
      "airport_fee",
      "cbd_congestion_fee",
      "revenue_per_mile",
      "revenue_per_minute",
      "trip_mph",
      "is_peak_hour",
      "is_night",
      "is_weekend"
    )

    orderedCols.foldLeft(deduped) { (df, colName) =>
      if (df.columns.contains(colName)) df else df.withColumn(colName, lit(null))
    }.select(orderedCols.map(col): _*)
  }

  /** Weekly row-count / revenue summary for basic completeness checks. */
  private def weeklySummary(df: DataFrame): DataFrame = {
    df.groupBy("pickup_week_id", "pickup_week_start")
      .agg(
        count(lit(1)).as("row_count"),
        sum("total_amount").as("total_revenue"),
        sum("trip_distance").as("total_miles")
      )
      .withColumn("run_ts", current_timestamp())
  }

  def writeOutputs(curated: DataFrame, basePath: String): Unit = {
    val curatedPath = s"$basePath/curated"
    val weeklyPath  = s"$basePath/_run_summary_weekly"

    writeParquetIfRows(curated, curatedPath)

    val summary = weeklySummary(curated)
    writeParquetIfRows(summary, weeklyPath)
  }

  private def writeParquetIfRows(df: DataFrame, path: String): Unit = {
    if (df.limit(1).count() > 0)
      coalesce1(df).write.mode("overwrite").parquet(path)
    else
      println(s"Skipped Parquet write (no rows): $path")
  }

  private def coalesce1(df: DataFrame): DataFrame =
    if (df.rdd.partitions.length > 1) df.coalesce(1) else df
}
