ThisBuild / scalaVersion := "2.13.16"      // class requires 2.13

lazy val root = (project in file("."))
  .settings(
    name := "data-pipeline",
    version := "0.1.0",
    organization := "com.group",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
    )
  )

// Optional: build a single runnable jar
Compile / mainClass := Some("MainApp")
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _                            => MergeStrategy.first
}