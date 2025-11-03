package bronze

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

// Simple Spark test session you can reuse
trait SparkTestSession extends AnyFunSuite {
  implicit lazy val spark: SparkSession =
    SparkSession.builder()
      .appName("BronzeTaxiZoneAppTest")
      .master("local[*]")
      .getOrCreate()

  // Stop Spark when tests finish
  sys.addShutdownHook {
    if (!spark.sparkContext.isStopped) spark.stop()
  }
}

final class BronzeTaxiZoneTest extends SparkTestSession {

  import BronzeTaxiZoneApp._

  // Stable alias so implicits stay in scope reliably
  private val ss: SparkSession = spark
  import ss.implicits._

  private val taxiSchema = StructType(Seq(
    StructField("LocationID",   StringType, nullable = true),
    StructField("Borough",      StringType, nullable = true),
    StructField("Zone",         StringType, nullable = true),
    StructField("service_zone", StringType, nullable = true)
  ))

  /** Create a temp CSV file using the first 10 real TLC rows (inline). */
  private def writeRealTaxiCsv(): String = {
    val tmpDir  = Files.createTempDirectory("taxi_zone_real_csv").toFile
    val csvPath = Paths.get(tmpDir.getAbsolutePath, "taxi_zone_lookup.csv")
    val content =
      """LocationID,Borough,Zone,service_zone
        |1,EWR,Newark Airport,EWR
        |2,Queens,Jamaica Bay,Boro Zone
        |3,Bronx,Allerton/Pelham Gardens,Boro Zone
        |4,Manhattan,Alphabet City,Yellow Zone
        |5,Staten Island,Arden Heights,Green Zone
        |6,Staten Island,Arrochar/Fort Wadsworth,Green Zone
        |7,Queens,Astoria,Boro Zone
        |8,Queens,Astoria Park,Boro Zone
        |9,Queens,Auburndale,Boro Zone
        |10,Queens,Baisley Park,Boro Zone
        |""".stripMargin
    Files.write(csvPath, content.getBytes(StandardCharsets.UTF_8))
    tmpDir.getAbsolutePath
  }

  test("normalizeColumns trims and turns blanks into nulls") {
    val df = Seq(
      ("1", " Queens ", " Astoria ", " Boro Zone "),
      ("2", " ", "Astoria Park", null.asInstanceOf[String])
    ).toDF("LocationID","Borough","Zone","service_zone")

    val out = normalizeColumns(df, Seq("Borough","Zone","service_zone"))

    // " Queens " -> "Queens"
    assert(out.filter(col("Borough") === "Queens").count() == 1)
    // " " -> NULL
    assert(out.filter(col("Borough").isNull).count() == 1)
    // null stays null
    assert(out.filter(col("service_zone").isNull).count() == 1)
  }

  test("applyChecks adds chk_* flags and is schema-tolerant") {
    val base = writeRealTaxiCsv()
    val df   = spark.read.option("header","true").schema(taxiSchema).csv(base)

    // Add one deliberately bad row: LocationID = "xyz" (non-numeric)
    val badRowDF = Seq(("xyz","Queens","Astoria","Boro Zone"))
      .toDF("LocationID","Borough","Zone","service_zone")

    val withBad = df.unionByName(badRowDF)

    val checked = applyChecks(withBad)
    assert(checked.columns.exists(_.startsWith("chk_")), "expected chk_* columns to be present")

    // That row should fail the numeric check
    val failFlag = checked
      .filter(col("LocationID") === "xyz")
      .select(col("chk_LocationID_numeric"))
      .collect()
      .head
      .getBoolean(0)

    assert(failFlag == false, "non-numeric LocationID must fail chk_LocationID_numeric")
  }

  test("summarizeChecks and computeNullPercents produce expected schemas") {
    val base = writeRealTaxiCsv()
    val df   = spark.read.option("header","true").schema(taxiSchema).csv(base)
    val checked = applyChecks(df)

    val summary = summarizeChecks(checked)
    val nulls   = computeNullPercents(checked, Seq("Borough","Zone","service_zone"))

    // No encoders needed: read via getString
    val metrics: Set[String] =
      summary.select("metric").collect().map(_.getString(0)).toSet

    assert(metrics.contains("row_count_total"))
    assert(metrics.contains("row_count_pass"))
    assert(metrics.contains("row_count_reject"))

    val nullMetrics: Array[String] =
      nulls.select("metric").collect().map(_.getString(0))

    assert(nullMetrics.exists(_.startsWith("null_pct_Borough")))
    assert(nullMetrics.exists(_.startsWith("null_pct_Zone")))
    assert(nullMetrics.exists(_.startsWith("null_pct_service_zone")))
  }
}