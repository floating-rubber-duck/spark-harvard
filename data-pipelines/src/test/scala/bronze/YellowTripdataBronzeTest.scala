package bronze

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import testsupport.SparkTestSession

/** Unit tests for YellowTripdataBronzeTripsApp helpers. */
final class YellowTripdataBronzeTest extends SparkTestSession {

  // Bring the app helpers into scope
  import YellowTripdataBronzeTripsApp._

  // Stable alias for implicits
  private val ss: SparkSession = spark
  import ss.implicits._

  test("normalizeStringCols trims blanks to null") {
    val df = Seq("Y ", " ", null.asInstanceOf[String]).toDF("store_and_fwd_flag")

    val out  = normalizeStringCols(df, Seq("store_and_fwd_flag"))
    val vals = out.select($"store_and_fwd_flag").as[String].collect().toSeq

    assert(vals(0) == "Y")
    assert(vals(1) == null)
    assert(vals(2) == null)
  }

  test("applyChecks adds *_type_ok flags and does not throw") {
    val df = Seq(
      (1,  "2025-01-01T00:00:00", "2025-01-01T00:05:00", "1", "0.5", "1", "Y", "1", "2", "1",
       "10.0","0.0","0.5","2.0","0.0","0.3","12.8","2.5","1.25","0.0")
    ).toDF(
      "VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance",
      "RatecodeID","store_and_fwd_flag","PULocationID","DOLocationID","payment_type",
      "fare_amount","extra","mta_tax","tip_amount","tolls_amount",
      "improvement_surcharge","total_amount","congestion_surcharge","airport_fee","cbd_congestion_fee"
    )

    val typed = applyChecks(
      df
        .withColumn("tpep_pickup_datetime",  to_timestamp($"tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", to_timestamp($"tpep_dropoff_datetime"))
        .withColumn("passenger_count",       $"passenger_count".cast("bigint"))
        .withColumn("trip_distance",         $"trip_distance".cast("double"))
        .withColumn("RatecodeID",            $"RatecodeID".cast("bigint"))
        .withColumn("PULocationID",          $"PULocationID".cast("int"))
        .withColumn("DOLocationID",          $"DOLocationID".cast("int"))
        .withColumn("payment_type",          $"payment_type".cast("bigint"))
        .withColumn("fare_amount",           $"fare_amount".cast("double"))
        .withColumn("extra",                 $"extra".cast("double"))
        .withColumn("mta_tax",               $"mta_tax".cast("double"))
        .withColumn("tip_amount",            $"tip_amount".cast("double"))
        .withColumn("tolls_amount",          $"tolls_amount".cast("double"))
        .withColumn("improvement_surcharge", $"improvement_surcharge".cast("double"))
        .withColumn("total_amount",          $"total_amount".cast("double"))
        .withColumn("congestion_surcharge",  $"congestion_surcharge".cast("double"))
        .withColumn("airport_fee",           $"airport_fee".cast("double"))
        .withColumn("cbd_congestion_fee",    $"cbd_congestion_fee".cast("double"))
    ) // <-- only one closing paren

    assert(typed.columns.exists(_.startsWith("chk_")))
  }

  test("addDomainAndRangeChecks tolerates bad input via TRY_CAST and never throws") {
    val df = Seq(
      // all strings (some invalid) — should not throw when checks run
      ("X","bad","bad","oops","-0.1","99","Z","-1","-2","9",
       "-10","x","y","z","-1","0.3","-100","x","x","x")
    ).toDF(
      "VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance",
      "RatecodeID","store_and_fwd_flag","PULocationID","DOLocationID","payment_type",
      "fare_amount","extra","mta_tax","tip_amount","tolls_amount",
      "improvement_surcharge","total_amount","congestion_surcharge","airport_fee","cbd_congestion_fee"
    )

    val typed   = applyChecks(df)                // type flags only
    val checked = addDomainAndRangeChecks(typed) // domain/range (uses TRY_CAST)

    assert(checked.columns.exists(_.startsWith("chk_")))
  }
}