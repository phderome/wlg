package phderome.challenge

import java.time.{LocalDateTime, ZoneId}

import scala.annotation.tailrec
import scala.collection.immutable.IndexedSeq
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
    def belongsToEarlierWindow(baseEpoch: Long)(item: (ClientAttributes, Int)): Boolean =
      item._2 == 0 || item._1.epochNanosecs <= baseEpoch + windowTimeSpanAsNanos

    // timeSequencedItems must be monotonically increasing by time (epochNanosecs).
    @tailrec
    def goSessionize(acc: IndexedSeq[(Long, IndexedSeq[ClientAttributes])],
                     timeSequencedItems: IndexedSeq[(ClientAttributes, Int)]): IndexedSeq[(Long, IndexedSeq[ClientAttributes])] = {
      val baseEpoch = timeSequencedItems.head._1.epochNanosecs
      val (current, next) = timeSequencedItems.span(belongsToEarlierWindow(baseEpoch))
      val appendable = (baseEpoch, current.map(_._1))
      if (next.isEmpty) acc :+ appendable
      else goSessionize(acc :+ appendable, next.map(_._1).zipWithIndex)
    }

    val timeSequencedItems = items.toIndexedSeq.sortBy(_.epochNanosecs)
    goSessionize(IndexedSeq.empty, timeSequencedItems.zipWithIndex)
      .map(x => SessionWindow(SessionKey(client, x._1), x._2.toArray)).toIterator
  }
}
