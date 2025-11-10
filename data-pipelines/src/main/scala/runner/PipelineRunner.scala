package runner

object PipelineRunner {
  def main(args: Array[String]): Unit = {
    val jobFromArgs = parseJobArg(args)
    val job = jobFromArgs
      .orElse(sys.props.get("job"))
      .getOrElse("yellow_trips")
      .trim
      .toLowerCase
    val passThrough = args.filterNot(_.equalsIgnoreCase("--job"))
                          .filterNot(_.equalsIgnoreCase(jobFromArgs.getOrElse("")))

    job match {
      case "yellow_trips" | "yellow" | "trips" =>
        bronze.YellowTripdataBronzeTripsApp.main(passThrough)  // trips job

      case "taxi_zone" | "zones" | "zone" =>
        bronze.BronzeTaxiZoneApp.main(passThrough)             // <-- App (not BronzeTaxiZone)

      case "yellow_trips_silver" | "silver_trips" | "silver" =>
        silver.YellowTripdataSilverApp.main(passThrough)

      case "show_silver" | "silver_preview" =>
        silver.SilverPreviewApp.main(passThrough)

      case other =>
        System.err.println(
          s"""Unknown job: '$other'
             |Use:
             |  -Djob=yellow_trips   to run the Yellow trips bronze job
             |  -Djob=taxi_zone      to run the Taxi Zone bronze job
             |  -Djob=yellow_trips_silver to run the Yellow trips silver job
             |  -Djob=show_silver    to preview Silver outputs
             |Or pass --job <name> as the first argument.
             |""".stripMargin)
        sys.exit(2)
    }
  }

  private def parseJobArg(args: Array[String]): Option[String] = {
    val idx = args.indexWhere(_.equalsIgnoreCase("--job"))
    if (idx >= 0 && idx + 1 < args.length) Some(args(idx + 1))
    else None
  }
}
