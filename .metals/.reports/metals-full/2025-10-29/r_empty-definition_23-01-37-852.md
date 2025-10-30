error id: file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala:`<none>`.
file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -stop.
	 -stop#
	 -stop().
	 -scala/Predef.stop.
	 -scala/Predef.stop#
	 -scala/Predef.stop().
offset: 863
uri: file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala
text:
```scala
package bronze

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.types._
import java.time.Instant
import java.util.UUID

object BronzeApp {

  // ---- Inputs (match your screenshot)
  val tripsIn   = "data/raw/yellow_tripdata_2025-01.parquet"
  val zonesCsv  = "data/raw/taxi_zones/taxi_zone_lookup.csv" 


  // ---- Bronze outputs
  val tripsOut  = "data/bronze/yellow_tripdata/"
  val zonesOut  = "data/bronze/taxi_zones/"
  val summaryOut= "data/bronze/_run_summary.json"

  val raw = spark.read.parquet(in)

  val df = raw
    .select(
        F.col("locationID"),
        F.col("Borough"),
        F.col("Zone"),
        F.col("service_zone")
    )
  
  df.write 
    .mode("overwrite")
    .partitionBy("Borough")
    .parquest(out)

    println(s"✅ Zones Bronze wrote ${df.count()} rows to $out")
    spark.@@stop()


}
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.