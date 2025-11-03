// ============================================================
// Project Settings
// ============================================================
ThisBuild / organization := "com.group"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.13.16"

// ============================================================
// Spark + Scala Configuration
// ============================================================
lazy val sparkVersion = "4.0.0"
lazy val useProvided: Boolean = sys.props.get("provided").exists(_.toBoolean)
def depScope = if (useProvided) "provided" else "compile"

// ============================================================
// Root Project Definition
// ============================================================
lazy val root = (project in file("."))
  .settings(
    name := "data-pipelines",

    // ------------------------------------------------------------
    // Library Dependencies
    // ------------------------------------------------------------
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % depScope,
      "org.apache.spark" %% "spark-sql"  % sparkVersion % depScope,
      "org.scala-lang"    % "scala-reflect" % scalaVersion.value,
      "org.scalatest"    %% "scalatest"  % "3.2.19" % Test
    ),

    // ------------------------------------------------------------
    // Application Entry Point
    // ------------------------------------------------------------
    Compile / mainClass := Some("runner.PipelineRunner"),

    // ------------------------------------------------------------
    // Local Dev JVM settings
    // ------------------------------------------------------------
    Compile / fork := true,
    Compile / javaOptions ++= Seq("-Xmx2g"),

    // ------------------------------------------------------------
    // Test Runtime Configuration
    // ------------------------------------------------------------
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq("-Xmx2g"),

    // Ensure ScalaTest is the framework & show detailed output
    Test / testFrameworks := Seq(new TestFramework("org.scalatest.tools.Framework")),
    Test / testOptions    ++= Seq(Tests.Argument(TestFrameworks.ScalaTest, "-oDF"))
  )

// ============================================================
// Assembly Settings
// ============================================================
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
import sbtassembly.PathList

assembly / test := {}

assembly / assemblyMergeStrategy := {
  case p if p.endsWith("module-info.class") => MergeStrategy.discard
  case PathList("META-INF", _ @ _*)         => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}