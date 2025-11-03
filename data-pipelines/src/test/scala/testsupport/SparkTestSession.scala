package testsupport

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Shared Spark test harness.
 * - Provides a STABLE `val spark` so `import spark.implicits._` works.
 * - Spins Spark up once per suite and tears it down after.
 */
trait SparkTestSession extends AnyFunSuite with BeforeAndAfterAll {

  // Build once and reuse
  private lazy val _session: SparkSession =
    SparkSession.builder()
      .appName("data-pipelines-tests")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  /** Stable identifier required by SQLImplicits. */
  protected final val spark: SparkSession = _session

  override protected def afterAll(): Unit = {
    try {
      _session.stop()
    } finally {
      super.afterAll()
    }
  }
}