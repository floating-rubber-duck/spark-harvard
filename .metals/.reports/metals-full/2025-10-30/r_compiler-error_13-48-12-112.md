file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/yellow_trips_bronze.scala
### java.lang.AssertionError: NoDenotation.owner

occurred in the presentation compiler.

presentation compiler configuration:


action parameters:
uri: file://<WORKSPACE>/data-pipelines/src/main/scala/bronze/yellow_trips_bronze.scala
text:
```scala
package bronze

import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * ============================================================
 *  YellowTripdataBronzeTripsApp
 *  ------------------------------------------------------------
 *  This Spark job reads the raw "yellow_tripdata_2025-01.parquet"
 *  file from the raw zone, applies schema-wide data quality (DQ)
 *  checks (type validations), computes null % per column,
 *  and outputs clean + reject datasets to the bronze zone as CSV.
 *
 *  Author: Zachary Gacer
 *  Date:   October 2025
 * ============================================================
 */
object YellowTripdataBronzeTripsApp {

  def main(args: Array[String]): Unit = {
    // Create SparkSession with local master (runs on all cores of your laptop)
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("BronzeApp")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // INPUT / OUTPUT PATHS
    // ------------------------------------------------------------
    val inputPath  = "data/raw/yellow_tripdata_2025-01.parquet"  // raw parquet input
    val outputPath = "data/bronze/bronze_yellow_tripdata"        // bronze CSV output base folder

    // ------------------------------------------------------------
    // STEP 1 — READ RAW DATA
    // ------------------------------------------------------------
    val raw = spark.read.parquet(inputPath)
    println("📘 Loaded input schema:")
    raw.printSchema()

    // ------------------------------------------------------------
    // STEP 2 — OPTIONAL CLEANUP (string normalization)
    // ------------------------------------------------------------
    // This trims whitespace and converts empty strings to NULLs.
    // Currently, only applied to "store_and_fwd_flag".
    val cleaned = normalizeStringCols(raw, Seq("store_and_fwd_flag"))

    // ------------------------------------------------------------
    // STEP 3 — APPLY DQ CHECKS (schema-tolerant)
    // ------------------------------------------------------------
    // Adds a boolean column for each field, like:
    //   chk_VendorID_type_ok = TRUE if (VendorID IS NULL) OR (cast(VendorID as Integer) succeeds)
    // The logic allows NULLs because every field in your schema is nullable ("YES").
    val withChecks = applyChecks(cleaned)

    // ------------------------------------------------------------
    // STEP 4 — AGGREGATE CHECK RESULTS
    // ------------------------------------------------------------
    // Combine all DQ flags into a master boolean column "dq_pass".
    // A row passes if *all* individual checks are TRUE.
    val checkCols = withChecks.columns.filter(_.startsWith("chk_"))
    val flagged =
      if (checkCols.isEmpty) withChecks.withColumn("dq_pass", lit(true))
      else withChecks.withColumn("dq_pass", checkCols.map(col).reduce(_ && _))

    // Split into passing and rejected rows
    val passes  = flagged.filter(col("dq_pass"))
    val rejects = flagged.filter(!col("dq_pass"))

    // ------------------------------------------------------------
    // STEP 5 — WRITE OUTPUTS TO BRONZE (CSV FORMAT)
    // ------------------------------------------------------------
    // All CSVs overwrite existing data for repeatable runs.
    // Pass dataset excludes chk_* columns for cleanliness.
    val passOut = passes.drop(("dq_pass" +: checkCols): _*)
    writeCSVIfRows(passOut, s"$outputPath/pass")
    writeCSVIfRows(rejects, s"$outputPath/_rejects")

    // ------------------------------------------------------------
    // STEP 6 — SUMMARIES (ROW COUNTS + NULL PERCENTAGES)
    // ------------------------------------------------------------
    val counts = summarizeChecks(flagged)
    val nulls  = computeNullPercents(flagged, tripSpec.map(_._1))
    val allSummary = if (nulls.head(1).isEmpty) counts else counts.unionByName(nulls)

    // Write CSV summaries (1 file per metric type)
    writeCSVIfSchema(coalesce1(counts),     s"$outputPath/_run_summary_counts")
    writeCSVIfSchema(coalesce1(nulls),      s"$outputPath/_run_summary_nulls")
    writeCSVIfSchema(coalesce1(allSummary), s"$outputPath/_run_summary_all")

    println(s"✅ Bronze trips written to: $outputPath")
    if (rejects.head(1).nonEmpty) println(s"⚠️ Rejects found at: $outputPath/_rejects")

    spark.stop()
  }

  // ============================================================
  // HELPER DEFINITIONS
  // ============================================================

  /**
   * Full logical schema of the Yellow Taxi dataset.
   * Each tuple = (column_name, expected_type, nullable_flag)
   */
  private val tripSpec: Seq[(String, String, Boolean)] = Seq(
    ("VendorID",               "INTEGER",   true),
    ("tpep_pickup_datetime",   "TIMESTAMP", true),
    ("tpep_dropoff_datetime",  "TIMESTAMP", true),
    ("passenger_count",        "BIGINT",    true),
    ("trip_distance",          "DOUBLE",    true),
    ("RatecodeID",             "BIGINT",    true),
    ("store_and_fwd_flag",     "VARCHAR",   true),
    ("PULocationID",           "INTEGER",   true),
    ("DOLocationID",           "INTEGER",   true),
    ("payment_type",           "BIGINT",    true),
    ("fare_amount",            "DOUBLE",    true),
    ("extra",                  "DOUBLE",    true),
    ("mta_tax",                "DOUBLE",    true),
    ("tip_amount",             "DOUBLE",    true),
    ("tolls_amount",           "DOUBLE",    true),
    ("improvement_surcharge",  "DOUBLE",    true),
    ("total_amount",           "DOUBLE",    true),
    ("congestion_surcharge",   "DOUBLE",    true),
    ("Airport_fee",            "DOUBLE",    true),
    ("cbd_congestion_fee",     "DOUBLE",    true)
  )

  /**
   * Map schema type strings to Spark SQL DataTypes.
   * Used by DQ casting checks.
   */
  private def toSparkType(t: String): DataType = t.toUpperCase match {
    case "INTEGER"   => IntegerType
    case "BIGINT"    => LongType
    case "DOUBLE"    => DoubleType
    case "TIMESTAMP" => TimestampType
    case "VARCHAR"   => StringType
    case _           => StringType // fallback to string
  }

  /**
   * Normalize simple string columns.
   * - Trims whitespace
   * - Converts empty strings ("") → NULL
   */
  private def normalizeStringCols(df: DataFrame, cols: Seq[String]): DataFrame =
    cols.foldLeft(df){ (d, c) =>
      if (d.columns.contains(c))
        d.withColumn(c,
          when(trim(col(c)) === "" || col(c).isNull, lit(null:String))
            .otherwise(trim(col(c)))
        )
      else d
    }

  /**
   * Apply DQ checks for the entire schema.
   * For each column:
   *   - If present in the DataFrame, add a boolean column chk_<col>_type_ok
   *     which passes if:
   *        value IS NULL  OR  value can be safely cast to expected type.
   *   - If missing, create chk_<col>_type_ok = TRUE (schema drift tolerance)
   *
   * Example:
   *   chk_trip_distance_type_ok = TRUE if trip_distance is null or cast(Double) works
   */
  private def applyChecks(df: DataFrame): DataFrame = {
    tripSpec.foldLeft(df) { case (tmp, (colName, typeStr, _nullable)) =>
      val chk = s"chk_${colName}_type_ok"
      if (tmp.columns.contains(colName)) {
        val dt = toSparkType(typeStr)
        tmp.withColumn(chk, col(colName).isNull || col(colName).cast(dt).isNotNull)
      } else {
        tmp.withColumn(chk, lit(true))
      }
    }
  }

  /**
   * Compute total, pass, and reject counts.
   * Returns a 3-row DataFrame with (metric, value, run_ts).
   */
  private def summarizeChecks(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val checkCols = df.columns.filter(_.startsWith("chk_"))
    val total = df.count()
    val pass  = if (checkCols.isEmpty) total else df.filter(checkCols.map(col).reduce(_ && _)).count()
    val rej   = total - pass
    Seq(
      ("row_count_total",  total.toString),
      ("row_count_pass",   pass.toString),
      ("row_count_reject", rej.toString)
    ).toDF("metric","value").withColumn("run_ts", current_timestamp())
  }

  /**
   * Compute null percentages per column.
   * Output format:
   *   metric = "null_pct_<column_name>"
   *   value  = ratio (0.0 → 1.0)
   */
  private def computeNullPercents(df: DataFrame, cols: Seq[String])
                                 (implicit spark: SparkSession): DataFrame = {
    val pieces = cols.filter(df.columns.contains).map { c =>
      df.agg(
        (sum(when(col(c).isNull, 1).otherwise(0)).cast("double") / count(lit(1))).as("v")
      ).select(lit(s"null_pct_$c").as("metric"), col("v").cast(StringType).as("value"))
       .withColumn("run_ts", current_timestamp())
    }
    pieces.reduceOption(_.unionByName(_)).getOrElse {
      import spark.implicits._
      Seq.empty[(String,String,java.sql.Timestamp)].toDF("metric","value","run_ts")
    }
  }

  /**
   * Coalesce (combine) all partitions into 1 before writing a single CSV file.
   */
  private def coalesce1(df: DataFrame): DataFrame =
    if (df.rdd.partitions.size > 1) df.coalesce(1) else df

  /**
   * Write CSV only if DataFrame has rows.
   * Used for main data outputs (pass/reject).
   */
  private def writeCSVIfRows(df: DataFrame, path: String): Unit =
    if (!df.head(1).isEmpty)
      df.coalesce(1).write.mode("overwrite").option("header","true").csv(path)
    else println(s"⏭️ Skipped CSV write (no rows): $path")

  /**
   * Write CSV only if DataFrame schema is non-empty.
   * Used for metric summaries.
   */
  private def writeCSVIfSchema(df: DataFrame, path: String): Unit =
    if (df.schema.nonEmpty)
      df.coalesce(1).write.mode("overwrite").option("header","true").csv(path)
    else println(s"⏭️ Skipped CSV write (empty schema): $path")
}
```



#### Error stacktrace:

```
dotty.tools.dotc.core.SymDenotations$NoDenotation$.owner(SymDenotations.scala:2609)
	dotty.tools.dotc.core.SymDenotations$SymDenotation.isSelfSym(SymDenotations.scala:715)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:330)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.fold$1(Trees.scala:1636)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.apply(Trees.scala:1638)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1669)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1771)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:457)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1677)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1771)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:457)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.fold$1(Trees.scala:1636)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.apply(Trees.scala:1638)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1675)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1771)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:457)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$13(ExtractSemanticDB.scala:391)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:334)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:386)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:454)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1724)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1771)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:354)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$11(ExtractSemanticDB.scala:377)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:334)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:377)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.apply(Trees.scala:1770)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1728)
	dotty.tools.dotc.ast.Trees$Instance$TreeAccumulator.foldOver(Trees.scala:1642)
	dotty.tools.dotc.ast.Trees$Instance$TreeTraverser.traverseChildren(Trees.scala:1771)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:351)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse$$anonfun$1(ExtractSemanticDB.scala:315)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:334)
	dotty.tools.dotc.semanticdb.ExtractSemanticDB$Extractor.traverse(ExtractSemanticDB.scala:315)
	dotty.tools.pc.SemanticdbTextDocumentProvider.textDocument(SemanticdbTextDocumentProvider.scala:36)
	dotty.tools.pc.ScalaPresentationCompiler.semanticdbTextDocument$$anonfun$1(ScalaPresentationCompiler.scala:242)
```
#### Short summary: 

java.lang.AssertionError: NoDenotation.owner