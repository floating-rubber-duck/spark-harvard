error id: file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala:coalesce.
file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala
empty definition using pc, found symbol in pc: coalesce.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions/summary/coalesce.
	 -org/apache/spark/sql/functions/summary/coalesce#
	 -org/apache/spark/sql/functions/summary/coalesce().
	 -org/apache/spark/sql/types/summary/coalesce.
	 -org/apache/spark/sql/types/summary/coalesce#
	 -org/apache/spark/sql/types/summary/coalesce().
	 -summary/coalesce.
	 -summary/coalesce#
	 -summary/coalesce().
	 -scala/Predef.summary.coalesce.
	 -scala/Predef.summary.coalesce#
	 -scala/Predef.summary.coalesce().
offset: 5155
uri: file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/bronze.scala
text:
```scala
package bronze

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import java.nio.file.{Files, Paths}

object YellowTripdataBronzeApp {

  // ---------- small utilities ----------
  private def hasCols(df: DataFrame, cols: Seq[String]): Boolean =
    cols.forall(df.columns.contains)

  private def addGuardedCheck(df: DataFrame, name: String, req: Seq[String])
                             (cond: DataFrame => Column): DataFrame =
    if (hasCols(df, req)) df.withColumn(name, cond(df)) else df.withColumn(name, lit(true))

  private def basicCountsKVs(total: Long, pass: Long, reject: Long)
                            (implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val now = current_timestamp()
    Seq(
      ("row_count_total", total.toString),
      ("row_count_pass",  pass.toString),
      ("row_count_reject", reject.toString)
    ).toDF("metric","value").withColumn("run_ts", now)
  }

  private def checkFailuresKVs(df: DataFrame, checks: Seq[String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    val now = current_timestamp()
    checks.map { c =>
      df.agg(sum(when(!col(c), 1).otherwise(0)).as("v"))
        .select(lit(s"fail_$c").as("metric"), col("v").cast(StringType).as("value"))
        .withColumn("run_ts", now)
    }.reduce(_.unionByName(_))
  }

  private def nullPercentsKVs(df: DataFrame, cols: Seq[String]): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    val now = current_timestamp()
    val pieces = cols.filter(df.columns.contains).map { c =>
      df.agg(
        (sum(when(col(c).isNull || col(c) === "", 1).otherwise(0)).cast("double") / count(lit(1)))
          .as("v")
      ).select(lit(s"null_pct_$c").as("metric"), col("v").cast(StringType).as("value"))
       .withColumn("run_ts", now)
    }
    pieces.reduceOption(_.unionByName(_)).getOrElse(spark.emptyDataFrame)
  }

  private def unionIfNonEmpty(base: DataFrame, extra: DataFrame): DataFrame =
    if (extra.head(1).isEmpty) base else base.unionByName(extra)

  // ---------- app ----------
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("YellowTripdataBronze")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val in  = args.lift(0).getOrElse("data/raw/yellow_tripdata_2025-01.parquet")
    val out = args.lift(1).getOrElse("data/bronze/yellow_tripdata/bronze_output")

    require(Files.exists(Paths.get(in)), s"Input not found: $in")

    // ---------- read & minimal shaping ----------
    val base = spark.read.parquet(in)
      .withColumn("LocationID",
        coalesce(col("PULocationID"), col("DOLocationID")).cast(StringType)
      )
      // keep schema stable; avoid Parquet VOID for null-typed cols
      .withColumn("Borough",      lit(null).cast(StringType))
      .withColumn("Zone",         lit(null).cast(StringType))
      .withColumn("service_zone", lit(null).cast(StringType))

    // ---------- declare DQ checks once ----------
    val checkDefs: Seq[(String, Seq[String], DataFrame => Column)] = Seq(
      ("chk_trip_distance_range",   Seq("trip_distance"),
        df => df("trip_distance").cast(DoubleType).between(0.0, 200.0)),
      ("chk_total_amount_range",    Seq("total_amount"),
        df => df("total_amount").cast(DoubleType).between(-20.0, 1000.0)),
      ("chk_passenger_count_range", Seq("passenger_count"),
        df => df("passenger_count").cast(IntegerType).between(0, 6)),
      ("chk_pickup_before_dropoff", Seq("tpep_pickup_datetime","tpep_dropoff_datetime"),
        df => df("tpep_pickup_datetime") < df("tpep_dropoff_datetime"))
    )

    // ---------- apply checks generically ----------
    val withChecks = checkDefs.foldLeft(base){ case (d, (name, req, pred)) =>
      addGuardedCheck(d, name, req)(pred)
    }
    val checkCols = checkDefs.map(_._1)

    // overall pass flag
    val bronzeDF = withChecks.withColumn("dq_pass", checkCols.map(col).reduce(_ && _))
    val passes   = bronzeDF.filter(col("dq_pass"))
    val rejects  = bronzeDF.filter(!col("dq_pass"))

    // ---------- writes ----------
    // keep all original columns but drop the DQ helper columns from bronze output
    val colsToDrop = checkCols :+ "dq_pass"
    val passOut =
      colsToDrop.foldLeft(passes)((d, c) => if (d.columns.contains(c)) d.drop(c) else d)

    passOut.write.mode("overwrite").parquet(out)

    if (rejects.head(1).nonEmpty)
      rejects.write.mode("overwrite").parquet(s"$out/_rejects")

    // ---------- run summary ----------
    val total  = bronzeDF.count()
    val passCt = passOut.count()          // count on cleaned output
    val rejCt  = total - passCt

    val counts   = basicCountsKVs(total, passCt, rejCt)
    val failures = checkFailuresKVs(bronzeDF, checkCols)
    val nulls    = nullPercentsKVs(bronzeDF, Seq("LocationID","Borough","Zone","service_zone"))

    val summary  = unionIfNonEmpty(unionIfNonEmpty(counts, failures), nulls)

    summary.coal@@esce(1).write.mode("overwrite").json(s"$out/_run_summary")

    println(s"✅ Bronze wrote $passCt rows to $out")
    if (rejCt > 0) println(s"⚠️ Rejected rows: $rejCt -> $out/_rejects")
    println(s"🧾 Run summary at $out/_run_summary")

    spark.stop()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: coalesce.