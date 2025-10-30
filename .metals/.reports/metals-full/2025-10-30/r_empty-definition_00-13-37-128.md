error id: file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala:read.
file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala
empty definition using pc, found symbol in pc: read.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/spark/read.
	 -spark/read.
	 -scala/Predef.spark.read.
offset: 685
uri: file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala
text:
```scala
package bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}

object YellowTripdataBronzeApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("YellowTripdataBronze")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val in  = args.lift(0).getOrElse("data/raw/yellow_tripdata_2025-01.parquet")
    val out = args.lift(1).getOrElse("data/bronze_output")

    require(Files.exists(Paths.get(in)), s"Input not found: $in")

    // Choose pickup as the LocationID source (fallback to dropoff if missing)
    val df = spark.r@@ead.parquet(in)
      .withColumn("LocationID",
        coalesce(col("PULocationID"), col("DOLocationID")).cast("string")
      )
      // Stub columns to be populated by a later join with the zones lookup
      .withColumn("Borough", lit(null:String))
      .withColumn("Zone", lit(null:String))
      .withColumn("service_zone", lit(null:String))
      .select("LocationID", "Borough", "Zone", "service_zone")

    df.write.mode("overwrite").parquet(out)

    println(s"✅ Bronze wrote ${df.count()} rows to $out")
    df.printSchema()
    spark.stop()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: read.