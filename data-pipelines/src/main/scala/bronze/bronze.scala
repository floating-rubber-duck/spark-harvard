package bronze
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BronzeApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BronzeIngestion")
      .master("local[*]")
      .getOrCreate()

    println("✅ Bronze stage started")

    // Sample dataset for testing
    import spark.implicits._
    val df = Seq(
      ("2025-01-01", "Manhattan", 25.5),
      ("2025-01-01", "Queens", 17.0)
    ).toDF("date", "borough", "fare")

    df.show()

    // Save output as Parquet (for next stage)
    df.write.mode("overwrite").parquet("data/bronze_output/")

    spark.stop()
    println("✅ Bronze stage complete")
  }
}