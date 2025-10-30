error id: file://<WORKSPACE>/data-pipelines/build.sbt:`<none>`.
file://<WORKSPACE>/data-pipelines/build.sbt
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -libraryDependencies.
	 -libraryDependencies#
	 -libraryDependencies().
	 -scala/Predef.libraryDependencies.
	 -scala/Predef.libraryDependencies#
	 -scala/Predef.libraryDependencies().
offset: 466
uri: file://<WORKSPACE>/data-pipelines/build.sbt
text:
```scala
ThisBuild / organization := "com.group"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.13.16"    // course requires 2.13

// Define the Spark version you use:
lazy val sparkVersion    = "4.0.0"       // if your runtime is Spark 4.x (Scala 2.13)
// lazy val sparkVersion = "3.5.1"       // use this instead if you’re on Spark 3.5.x (Scala 2.12)

lazy val root = (project in file("."))
  .settings(
    name := "data-pipelines",
    libraryDependen@@cies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
    ),
    Compile / mainClass := Some("bronze.BronzeApp")
  )

// Runnable JAR settings
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*)         => MergeStrategy.discard
  case p if p.endsWith("module-info.class") => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.