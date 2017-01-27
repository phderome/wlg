package phderome.challenge

import java.time.{LocalDateTime, ZoneId}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

case class ClientAccess(client: String, attributes: ClientAttributes)
case class ClientAttributes(epochNanosecs: Long, uri: String) {
  override def toString: String = s"($epochNanosecs, $uri)"
}
case class DurationAndSize(duration: Double, size: Double) {// allows convenient summing without overflow
  override def toString: String = f"($duration%6.9f, $size%f)"
}
case class SessionKey(client: String, epochNanosecs: Long) {
  override def toString: String = s"($client, $epochNanosecs)"
}
case class SessionWindow(sessionKey: SessionKey, data: Array[ClientAttributes]) {
  // data cannot use Iterable but Array (Encoder, serialization matters)
  override def toString: String = {
    val ds = data.mkString(",")
    s"($sessionKey, $ds)"
  }
}

case class SessionizedSummary(sessionKey: SessionKey, count: Long) {
  override def toString: String = s"($sessionKey, $count)"
}

case class ClientDurationTally(client: String, durationTotal: Double, durationSize: Long)
case class ClientDurationAverage(client: String, durationAverage: Double) {
  def pretty: String = f"client: $client%s ${durationAverage/ClientAccess.billion}%5.6f"
}

object ClientAccess {
  val billion = 1000000000L // nanos per second
  val timeZoneTrailer = 'Z'
  val timeZoneFormatLength = 27
  val timeZoneFormatLastIndex: Int = timeZoneFormatLength - 1
  // We'll tokenize the line by space and tab
  val tokenDelims = "\\s+"
  val ipPortDelim = ":"
  val timestampTokenIndex = 0
  val clientIpTokenIndex = 2
  val uriTokenIndex = 12
  val requiredValidTokensPerLine = 13  // could be a larger number than 12 upon stricter analysis, ignore bad trailing data
  val requiredValidTokensLastIndex: Int = requiredValidTokensPerLine - 1

  def toClientAccess(line: String): Option[ClientAccess] = {
    def encodeLocalDateTime(ldt: LocalDateTime): Long = { // 64 bits can contain epochSeconds * Billion
      val atZone = ldt.atZone(ZoneId.systemDefault()) // we assume homogeneous TZ in the input data
      atZone.toEpochSecond * billion + atZone.getNano
    }

    for { // We flatMap the for comprehension over options.
      validTokens <- Some(line.split(tokenDelims).toIndexedSeq)
      if validTokens.size >= requiredValidTokensPerLine && line.length > timeZoneFormatLength
      epochNanosecs <- Try(LocalDateTime.parse(validTokens(timestampTokenIndex).substring(0, timeZoneFormatLastIndex - 1))).
        map(encodeLocalDateTime).toOption
      if validTokens(timestampTokenIndex)(timeZoneFormatLastIndex) == timeZoneTrailer
      clientIPTokens = validTokens(clientIpTokenIndex).split(ipPortDelim) if clientIPTokens.size == 2
      // expected to be <IP>:<port> and nothing else, just that.
    } yield ClientAccess(client = clientIPTokens(0), ClientAttributes(epochNanosecs, uri = validTokens(uriTokenIndex)))
  }

  // count the unique urls but prepends with 1 as a pair to facilitate reduction and counting number of windows.
  def getUniqueUrlsBySession(sessionWindow: SessionWindow): (Long, Long) =
    (1, sessionWindow.data.
      groupBy(_.uri).
      count(pair => pair._2.length == 1))

  def getCount(sessionWindow: SessionWindow): (String, Long) =
    (sessionWindow.sessionKey.client, sessionWindow.data.length)

  def createCDT(client: String, durs: Array[Double]): ClientDurationTally =
    ClientDurationTally(client, durs.sum, durs.length.toLong)

  // Assumes data is pre-sorted by epochNanosecs
  def getDurationAndSizeBySessionWithClient(sessionKey: SessionKey, data: Array[ClientAttributes]): Option[(String, DurationAndSize)] = {
    val length = data.length
    if (length < 2) None // On small data sets, we can avoid a division by 0 when reducing as we flatMap the Nones.
    else {
      val times = data.map(_.epochNanosecs) // no need to sort this. At least this piece is not slow.
      Some(sessionKey.client, DurationAndSize(times.max - times.min, length))
    }
  }

  // here we capture sessions of one element, but they'll be discarded later.
  def sessionize(client: String,
                 items: Iterator[ClientAttributes],
                 windowTimeSpanAsNanos: Long): Iterator[SessionWindow] = {
    // I would have liked to use Windowing functions with frames, something like org.apache.spark.sql.expressions.Window
    // Maybe reading up on Spark Streaming would work??? Highly imperative below... Seems like we're forced to use Arrays/ArrayBuffers here.
    // Don't fight the framework. A less imperative and simpler solution is found in unit test to validate the algorithm here.
    val timeSequencedItems = items.toIndexedSeq.sortBy(_.epochNanosecs)
    var currEpoch = timeSequencedItems.head.epochNanosecs // head is generally unsafe, but this is used in context of having done a groupByKey so safe.

    var currSession: (SessionKey, ArrayBuffer[ClientAttributes]) = (SessionKey(client, currEpoch), ArrayBuffer[ClientAttributes]())
    // this is initialized with nothing in ArrayBuffer of ClientAttributes because we use invariant that each entry is made by verifying against a reference
    // currEpoch for window time (even though we know here that orderedItems.head will qualify); this ensures we don't insert an element twice
    // when it should be inserted only once.

    val sessions = ArrayBuffer[(SessionKey, ArrayBuffer[ClientAttributes])]() // likewise, here we insert only complete SessionWindows
    // (not exactly a SessionWindow since we use mutable ArrayBuffer in place of immutable Array.

    val it = timeSequencedItems.iterator
    while(it.nonEmpty) {
      val attributes = it.next()
      if( currEpoch + windowTimeSpanAsNanos < attributes.epochNanosecs ) {
        currEpoch = attributes.epochNanosecs
        sessions ++= Array(currSession) // capture current session
        currSession = (SessionKey(client, currEpoch), ArrayBuffer(attributes)) // start a new one from current attributes
      }
      else {
        currSession._2 += attributes
      }
    }
    sessions ++= Array(currSession) // last session must be captured here since its epoch will qualify (found thanks to scalacheck!)
    sessions.map(x => SessionWindow(x._1, x._2.toArray)).toIterator
  }
}
