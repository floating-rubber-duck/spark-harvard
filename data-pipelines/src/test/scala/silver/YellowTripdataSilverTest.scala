package silver

import java.sql.Timestamp

import org.apache.spark.sql.types.{DoubleType, IntegerType, TimestampType}
import org.apache.spark.sql.functions._
import testsupport.SparkTestSession

final class YellowTripdataSilverTest extends SparkTestSession {

  import spark.implicits._
  import YellowTripdataSilverTransforms._

  private val samplePickup  = Timestamp.valueOf("2025-01-06 07:15:00")
  private val sampleDropoff = Timestamp.valueOf("2025-01-06 07:45:00")

  test("standardizeTrips renames columns and enforces target types") {
    val bronzeDf = Seq(
      (1, samplePickup, sampleDropoff, 2L, 3.5, 1L, "y", 132, 138, 1L,
        12.5, 0.5, 0.5, 2.5, 0.0, 0.3, 16.3, 2.5, 1.25, 0.0)
    ).toDF(
      "VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance",
      "RatecodeID","store_and_fwd_flag","PULocationID","DOLocationID","payment_type",
      "fare_amount","extra","mta_tax","tip_amount","tolls_amount",
      "improvement_surcharge","total_amount","congestion_surcharge","airport_fee","cbd_congestion_fee"
    )

    val standardized = standardizeTrips(bronzeDf)

    assert(standardized.columns.contains("pickup_datetime"))
    assert(standardized.schema("pickup_datetime").dataType == TimestampType)
    assert(standardized.schema("trip_distance").dataType == DoubleType)
    assert(standardized.schema("vendor_id").dataType == IntegerType)

    val flag = standardized.select($"store_and_fwd_flag").as[String].head()
    assert(flag == "Y") // was lowercase, should be upper-trimmed
  }

  test("joinTaxiZones adds pickup and dropoff borough metadata") {
    val trips = Seq(
      (132, 138, samplePickup, sampleDropoff, 1.0)
    ).toDF("pu_location_id","do_location_id","pickup_datetime","dropoff_datetime","trip_distance")

    val zones = Seq(
      ("132", "Queens",  "Astoria", "Boro Zone"),
      ("138", "Queens",  "Steinway", "Boro Zone")
    ).toDF("LocationID","Borough","Zone","service_zone")

    val joined = joinTaxiZones(trips, zones)

    val row = joined.select("pu_borough","do_borough").head()
    assert(row.getString(0) == "Queens")
    assert(row.getString(1) == "Queens")
  }

  test("addDerivedColumns computes temporal flags and revenue metrics") {
    val trips = Seq(
      ("ride", samplePickup, sampleDropoff, 5.0, 40.0)
    ).toDF("ride_id","pickup_datetime","dropoff_datetime","trip_distance","total_amount")

    val enriched = addDerivedColumns(trips)

    val out = enriched.select(
      $"pickup_week_id",
      $"trip_minutes",
      $"revenue_per_mile",
      $"is_peak_hour",
      $"is_night"
    ).head()

    assert(out.getString(0).startsWith("2025-W"))
    assert(math.abs(out.getDouble(1) - 30.0) < 0.001)
    assert(math.abs(out.getDouble(2) - (40.0 / 5.0)) < 0.001)
    assert(out.getBoolean(3)) // 7 AM => peak
    assert(!out.getBoolean(4)) // not night
  }

  test("finalizeDataset drops duplicates and keeps ordered columns") {
    val df = Seq(
      (samplePickup, sampleDropoff, 132, 138, 4.2, 25.0),
      (samplePickup, sampleDropoff, 132, 138, 4.2, 25.0) // duplicate
    ).toDF(
      "pickup_datetime","dropoff_datetime","pu_location_id","do_location_id","trip_distance","total_amount"
    )

    val prepared = addDerivedColumns(df)
    val finalized = finalizeDataset(prepared)

    assert(finalized.columns.head == "ride_id")
    assert(finalized.count() == 1)
  }
}
