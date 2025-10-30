package bronze

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types._
import java.time.Instant
import java.util.UUID

object BronzeApp {

  val inputPath = "data/raw/yellow_tripdata_2025-01.parquet"
  val bronzeOut = "data/bronze_output/yellow_tripdata_2025_01/"
  val summaryOutput = "data/bronze_output/_run_summary.json"
}