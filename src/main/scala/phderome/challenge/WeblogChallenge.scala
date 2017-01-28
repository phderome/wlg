package phderome.challenge

import java.io._

import ClientAccess._
import cats.data.Writer
import org.apache.spark.sql.{Dataset, _}

object WeblogChallenge {

  implicit class toW(s: String) {
    def ~>[A](ds: Dataset[A]): Writer[List[String], Dataset[A]] = Writer(s :: Nil, ds)
    def ~>[K, V](kvgd: KeyValueGroupedDataset[K, V]): Writer[List[String], KeyValueGroupedDataset[K, V]] = Writer(s :: Nil, kvgd)

  }

  case class Pipeline(engagedClients: Dataset[ClientDurationAverage],
                      sessionizedCounts: Dataset[(String, Long)],
                      uniqueUrlPerSessions: Dataset[(Long, Long)],
                      durationAndSizesPerSessionsDS: Dataset[ClientDurationAndSize])

  private def saveAggregate(fileName: String, line: String): Unit = {
    val pw = new PrintWriter(new File(fileName))
    pw.write(line)
    pw.close()
  }

  case class SaveToParquetParams[T](ds: Dataset[T], path: String, printHeader: String, take: Int)

  private def saveToParquet[T](params: SaveToParquetParams[T]): Unit = {
    params.ds
      .write
      .mode(SaveMode.Overwrite)
      .save(path = params.path)
    params.ds.take(params.take).foreach { s => println(s"*************************  ${params.printHeader}: $s") }
  }

  def aggregate[T](ds: Dataset[T],
                   zero: String,
                   one: T => String,
                   reduce: Dataset[T] => String): String = {
    ds.count() match {
      case 0 => zero //"Not enough data\n"
      case 1 => one(ds.take(1).apply(1)) //._2 f"$avgUrl%d 1 => avg unique per session $avgUrl%s\n"
      case _ => reduce(ds)
    }
  }

  def getSessionized(fileName: String, windowAsNanos: Long)(implicit spark: SparkSession): Writer[List[String], Dataset[SessionWindow]] = {
    import spark.implicits._
    "getSessionized" ~> spark.read.textFile(fileName)
      .flatMap(toClientAccess)
      .groupByKey(_.client)
      .mapValues(datum => ClientAttributes(datum.attributes.epochNanosecs, datum.attributes.uri))
      .flatMapGroups((client, accesses) => sessionize(client, accesses, windowAsNanos))
  }

  def getDurationAndSize(keyedSessionWindows: KeyValueGroupedDataset[SessionKey, SessionWindow])
                        (implicit spark: SparkSession): Writer[List[String], Dataset[ClientDurationAndSize]] = {
    import spark.implicits._
    "getDurationAndSize" ~> keyedSessionWindows
      .flatMapGroups((k, sWindows) => sWindows.flatMap(w => getDurationAndSizeBySessionWithClient(k, w.data)))
      .map { case (k,v) => ClientDurationAndSize(k, v) }
  }

  def getEngagedClients(ds: Dataset[ClientDurationAndSize])(implicit spark: SparkSession): Writer[List[String], Dataset[ClientDurationAverage]] = {
    import spark.implicits._
    "getEngagedClients" ~> ds
      .groupByKey(_.client) // consolidate/key by client and obtain iterable of (client, durations)
      .mapValues(_.durationAndSize.duration) // weed out client to keep only duration in iterables
      .mapGroups((client, durs) => createCDT(client, durs.toArray))
      .map(cdt => ClientDurationAverage(cdt.client, cdt.durationTotal / cdt.durationSize.toDouble)) // ClientDurationAverage
      .sort($"durationAverage".desc)
  }

  def getSessionizedCounts(ksws: KeyValueGroupedDataset[SessionKey, SessionWindow])
                          (implicit spark: SparkSession): Writer[List[String], Dataset[(String, Long)]] = {
    import spark.implicits._
    "getEngagedClients" ~> ksws
      .flatMapGroups((k, sWindows) => sWindows.map(getCount))
  }

  def getUniqueUrlPerSessions(ksws: KeyValueGroupedDataset[SessionKey, SessionWindow])
                             (implicit spark: SparkSession): Writer[List[String], Dataset[(Long, Long)]] = {
    import spark.implicits._
    "getEngagedClients" ~> ksws
      .flatMapGroups((k, sWindows) => sWindows.map(getUniqueUrlsBySession))
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
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Weblog Challenge")
      .getOrCreate()

    import spark.implicits._
    import cats.implicits._

    val pipeline = for {
      sessionized <- getSessionized(args(0), windowTimeSpanAsNanos)(spark)
      keyedSessionWindows = sessionized.groupByKey(_.sessionKey)
      durationAndSizesPerSessionsDS <- getDurationAndSize(keyedSessionWindows)
      engagedClients <- getEngagedClients(durationAndSizesPerSessionsDS)
      sessionizedCounts <- getSessionizedCounts(keyedSessionWindows)
      uniqueUrlPerSessions <- getUniqueUrlPerSessions(keyedSessionWindows)
    } yield Pipeline(engagedClients, sessionizedCounts, uniqueUrlPerSessions, durationAndSizesPerSessionsDS )

    val (theLog, theDatasets) = pipeline.run
    println(s"Pipeline Log is $theLog}")

    // Spark actions start below, after having done required transformations.
    val uniqueUrlLine: String = {
      def reduceTwoPlus(ds: Dataset[(Long, Long)]): String = {
        val (sessionCount: Long, urlCount: Long) = ds.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
        val avgUrl = urlCount.toDouble/sessionCount.toDouble
        f"$urlCount%d $sessionCount%d => avg unique per session $avgUrl%4.4f\n"
      }
      def reduceOne(pair: (Long, Long)): String = {
        val avgUrl: Long = pair._2
        f"$avgUrl%d 1 => avg unique per session $avgUrl%s\n"
      }
      aggregate(theDatasets.uniqueUrlPerSessions, "Not enough data\n", reduceOne, reduceTwoPlus)
    }

    val durationLine: String = {
      def reduceTwoPlus(ds: Dataset[ClientDurationAndSize]): String = {
        val durationsPerSessions = ds
          .map { _.durationAndSize}
          .reduce((a, b) => DurationAndSize(a.duration + b.duration, a.size + b.size))
        val avgDuration = durationsPerSessions.duration/(durationsPerSessions.size * billion)
        f"$avgDuration%6.6f seconds\n"
      }
      def reduceOne(d: ClientDurationAndSize): String = f"${d.durationAndSize.duration}%6.6f seconds\n"
      aggregate(theDatasets.durationAndSizesPerSessionsDS, "Not enough data\n", reduceOne, reduceTwoPlus )
    }

    // Outputs
    saveAggregate("output/UniqueUrl.txt", uniqueUrlLine)
    saveAggregate("output/AvgDuration.txt", durationLine)
    saveToParquet(SaveToParquetParams(theDatasets.engagedClients, "output/EngagedClients", "engaged", 20 ))
    saveToParquet(SaveToParquetParams(theDatasets.sessionizedCounts, "output/SessionizedCounts", "Sessionized Count", 20 ))
  }

}
