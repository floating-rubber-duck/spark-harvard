package runner

object PipelineRunner {

  private def usage(): Unit = {
    println(
      """Usage: sbt "run [command]"
        |Commands:
        |  all               Run all bronze jobs (default)
        |  yellow            Run bronze Yellow Tripdata (Trips) job
        |  taxi              Run bronze Taxi Zone job
        |  list              Show available jobs
        |""".stripMargin)
  }

  def main(args: Array[String]): Unit = {
    val cmd = args.headOption.map(_.toLowerCase).getOrElse("all")
    val passThrough = args.drop(1)

    cmd match {
      case "all" =>
        println("[Runner] Running ALL bronze jobs...")
        bronze.YellowTripdataBronzeTripsApp.main(passThrough) // from yellow_trips_bronze.scala
        bronze.YellowTripdataBronzeApp.main(passThrough)      // from bronze_taxi_zone.scala

      case "yellow" | "bronze:yellow" =>
        println("[Runner] Running Bronze Yellow Tripdata (Trips) job...")
        bronze.YellowTripdataBronzeTripsApp.main(passThrough)

      case "taxi" | "bronze:taxi" =>
        println("[Runner] Running Bronze Taxi Zone job...")
        bronze.YellowTripdataBronzeApp.main(passThrough)

      case "list" =>
        println("Available jobs:")
        println(" - yellow  (bronze.YellowTripdataBronzeTripsApp)")
        println(" - taxi    (bronze.YellowTripdataBronzeApp)")

      case "--help" | "-h" | "help" =>
        usage()

      case other =>
        println(s"[Runner] Unknown command: $other\n"); usage(); sys.exit(2)
    }
  }
}