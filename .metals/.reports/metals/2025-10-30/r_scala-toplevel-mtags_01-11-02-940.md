error id: file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala:[524..528) in Input.VirtualFile("file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala", "package bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.types.{StringType}   // <-- missing import


object YellowTripdataBronzeApp {

  private def hasCols(df: DataFrames, cols: Seq[String]): Boolean =
    cols.forall(df.columns.contains)

  private def addGuardedCheck(df, name, req)(cond) =
    if (hasCols(df, req))
      df.withColumn(name, cond(df))
    else
      df.withColumn(name, lit(true))
  
  private def null
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("YellowTripdataBronze")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val in  = args.lift(0).getOrElse("data/raw/yellow_tripdata_2025-01.parquet")
    val out = args.lift(1).getOrElse("data/bronze_output")

    //Check if the path exisits
    require(Files.exists(Paths.get(in)), s"Input not found: $in")
    require

    // Choose pickup as the LocationID source (fallback to dropoff if missing)
    val df = spark.read.parquet(in)
      // use pickup first, fallback to dropoff; ensure string type
      .withColumn("LocationID",
        coalesce(col("PULocationID"), col("DOLocationID")).cast(StringType)
      )
      // explicitly cast nulls to STRING so Parquet writer is happy
      .withColumn("Borough",      lit(null).cast(StringType))
      .withColumn("Zone",         lit(null).cast(StringType))
      .withColumn("service_zone", lit(null).cast(StringType))
      .select("LocationID", "Borough", "Zone", "service_zone")

    df.write.mode("overwrite").parquet(out)

    println(s"✅ Bronze wrote ${df.count()} rows to $out")
    df.printSchema()
    spark.stop()
  }
}")
file://<WORKSPACE>/file:<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala
file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala:20: error: expected identifier; obtained null
  private def null
              ^
#### Short summary: 

expected identifier; obtained null