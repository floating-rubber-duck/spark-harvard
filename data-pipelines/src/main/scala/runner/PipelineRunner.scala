package runner

object PipelineRunner {
  def main(args: Array[String]): Unit = {
    val job         = sys.props.getOrElse("job", "yellow_trips").trim.toLowerCase
    val passThrough = args

    job match {
      case "yellow_trips" | "yellow" | "trips" =>
        bronze.YellowTripdataBronzeTripsApp.main(passThrough)  // trips job

      case "taxi_zone" | "zones" | "zone" =>
        bronze.BronzeTaxiZoneApp.main(passThrough)             // <-- App (not BronzeTaxiZone)

      case other =>
        System.err.println(
          s"""Unknown job: '$other'
             |Use:
             |  -Djob=yellow_trips   to run the Yellow trips bronze job
             |  -Djob=taxi_zone      to run the Taxi Zone bronze job
             |""".stripMargin)
        sys.exit(2)
    }
  }
}