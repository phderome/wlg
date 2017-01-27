package phderome.challenge

import java.io._
import ClientAccess._

import org.apache.spark.sql._

object WeblogChallenge {

  private def saveAggregate(fileName: String, line: String): Unit = {
    val pw = new PrintWriter(new File(fileName ))
    pw.write(line)
    pw.close()
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      println(s"""Usage: spark-submit --class \"phderome.challenge.WeblogChallenge\"
        | --master local[4] target/scala-2.11/paytmphderome_2.11-1.0.jar <datafile> <window in secs>
        | if window in secs is not provided a default of 900 seconds or 15 minutes is chosen""")
      System.exit(1)
    }

    // we try to delay loss of accuracy to latest time possible, so we carry around the digits accuracy and operate on nanos.
    val windowTimeSpanAsNanos: Long = billion * (if (args.length >= 2) args(1).toInt else 15 * 60) // 15 minutes default
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Weblog Challenge")
      .getOrCreate()

    import spark.implicits._

    val accessesByClient: KeyValueGroupedDataset[String, ClientAttributes] = spark.read.textFile(args(0)).
      flatMap(toClientAccess).
      groupByKey(_.client).
      mapValues(datum => ClientAttributes(datum.attributes.epochNanosecs, datum.attributes.uri) )

    val sessionizeds: Dataset[SessionWindow] =
      accessesByClient.flatMapGroups((client, accesses) => sessionize(client, accesses, windowTimeSpanAsNanos)).
        cache()

    val keyedSessionWindows: KeyValueGroupedDataset[SessionKey, SessionWindow] = sessionizeds.groupByKey(_.sessionKey)
    val uniqueUrlPerSessions: Dataset[(Long, Long)] =
      keyedSessionWindows.
        flatMapGroups(  (k, sWindows) => sWindows.map(getUniqueUrlsBySession)  )
    val durationAndSizesPerSessionsDS: Dataset[(String, DurationAndSize)] =
      keyedSessionWindows.flatMapGroups(  (k, sWindows) => sWindows.flatMap( w => getDurationAndSizeBySessionWithClient(k, w.data) )   ).
        cache()

    val sessionizedCounts: Dataset[(String, Long)] = keyedSessionWindows.
      flatMapGroups(  (k, sWindows) => sWindows.map(getCount)  )

    // aggregate the durations for each client and count the number of sessions for each client along the way, finally operate the division by client
    val cDTs: Dataset[ClientDurationTally] = durationAndSizesPerSessionsDS.
      map{ case (client: String, ds: DurationAndSize) => (client, ds.duration)}.
      groupByKey(_._1). // consolidate/key by client and obtain iterable of (client, durations)
      mapValues(_._2). // weed out client to keep only duration in iterables
      mapGroups(  (client, durs) => createCDT(client, durs.toArray)  )

    val durationAveragesByClientDS: Dataset[ClientDurationAverage] =
      cDTs.map(cdt => ClientDurationAverage(cdt.client, cdt.durationTotal/cdt.durationSize.toDouble))

    val engagedClients: Dataset[ClientDurationAverage] = durationAveragesByClientDS.sort($"durationAverage".desc)

    // Spark actions start below, after having done required transformations.
    val uniqueUrlLine = uniqueUrlPerSessions.count() match {
      case 0 => "Not enough data\n"
      case 1 => val avgUrl: Long = uniqueUrlPerSessions.take(1).apply(1)._2
        f"$avgUrl%d 1 => avg unique per session $avgUrl%s\n"
      case _ => val (sessionCount: Long, urlCount: Long) = uniqueUrlPerSessions.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
        val avgUrl = urlCount.toDouble/sessionCount.toDouble
        f"$urlCount%d $sessionCount%d => avg unique per session $avgUrl%4.4f\n"
    }

    val durationLine: String =
      durationAndSizesPerSessionsDS.count() match {
        case 0 => "Not enough data\n"
        case 1 => val avgDuration = durationAndSizesPerSessionsDS.take(1).apply(1)._2.duration
          f"$avgDuration%6.6f seconds\n"
        case _ => // may reduce
          val durationsPerSessions = durationAndSizesPerSessionsDS.
            map { case (client: String, ds: DurationAndSize) => ds}.
            reduce((a, b) => DurationAndSize(a.duration + b.duration, a.size + b.size))
          val avgDuration = durationsPerSessions.duration/(durationsPerSessions.size * billion)
          f"$avgDuration%6.6f seconds\n"
      }

    saveAggregate("output/UniqueUrl.txt", uniqueUrlLine)
    saveAggregate("output/AvgDuration.txt", durationLine)

    engagedClients.
      write.
      mode(SaveMode.Overwrite).
      save(path = "output/EngagedClients")
    engagedClients.take(20).foreach{s => println(s"*************************  engaged: ${s.pretty}")}

    sessionizedCounts.
      write.
      mode(SaveMode.Overwrite).
      save(path = "output/SessionizedCounts")
    sessionizedCounts.take(20).foreach(s => println(s"************************* Sessionized Count: $s"))
  }

}
