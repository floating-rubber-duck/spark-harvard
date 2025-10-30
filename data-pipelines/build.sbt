// ---- Project settings ----
ThisBuild / organization := "com.group"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.13.16"

// Match your Spark runtime (4.x supports Scala 2.13)
lazy val sparkVersion = "4.0.0"

// Toggle: by default we include Spark on the classpath for sbt run.
// Pass -Dprovided=true when assembling for a real cluster.
lazy val useProvided: Boolean =
  sys.props.get("provided").exists(_.toBoolean)

// Helper to choose scope
def depScope = if (useProvided) "provided" else "compile"

lazy val root = (project in file("."))
  .settings(
    name := "data-pipelines",
    // deps
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % depScope,
      "org.apache.spark" %% "spark-sql"  % sparkVersion % depScope,
      "org.scala-lang"    % "scala-reflect" % scalaVersion.value
    ),
    // entrypoint (you can override with runMain)
    Compile / mainClass := Some("runner.PipelineRunner"),
    // smoother local dev
    Compile / fork := true,
    Compile / javaOptions ++= Seq("-Xmx2g")
  )

// ---- Assembly (fat JAR) ----
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case p if p.endsWith("module-info.class") => MergeStrategy.discard
  case PathList("META-INF", _ @ _*)         => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}