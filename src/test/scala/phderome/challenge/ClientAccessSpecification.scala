package phderome.challenge

import org.scalacheck.{Prop, _}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer
object ClientAccessSpecification extends Properties("ClientAccess") {
  import Prop.forAll
  import ClientAccess._

  val window: Gen[Int] = Gen.oneOf(2, 5, 10, 12, 20,73, 101, 999, 1000)
  val genClientAttribsList: Gen[List[ClientAttributes]] =
    Gen.containerOfN[List, ClientAttributes](15, Gen.choose(1,1000).map(i => ClientAttributes(i,
      "bob")))

  // initial solution had bug where we had some duplicate keys, make sure we never get those.
  property("sessionize does not duplicate session keys") = forAll(genClientAttribsList, window) { (attrs, w) =>
    val newSeq = attrs.toIndexedSeq
    val client = "sue"
    lazy val trial = sessionize(client, newSeq.toIterator, w).toSeq
    lazy val keys = trial.map(_.sessionKey)
    // results are not defined on empty sequences intentionally
    attrs.length < 1 || keys.size == keys.distinct.size
  }

  val isolatedWindowSize = 100
  val genIsolatedClientAttribsList: Gen[List[ClientAttributes]] =
    Gen.containerOfN[List, ClientAttributes](80, Gen.choose(1,10000).
      map(_ * (isolatedWindowSize + 1)).
      map(i => ClientAttributes(i, "one use in isolation")))

  property("sessionize does not bring in distinct times in same window when times are far apart") = forAll(genIsolatedClientAttribsList) { attrs =>
    val newSeq = attrs.distinct.toIndexedSeq
    val client = "one use in isolation"
    lazy val trial = sessionize(client, newSeq.toIterator, isolatedWindowSize).toSeq // window is one shorter than what we sample (101)
    lazy val values = trial.map(_.data)
    // results are not defined on empty sequences intentionally
    attrs.length < 1 || values.forall(_.length == 1)
  }

  // We define an alternative simple way of computing results (expectSessionize, which depend on methods defined earlier on below)
  // Then we use scalacheck to generate many combinations of sequences to show that the two methods arrive at the same result, with the simple
  // method defined in this test being easier to analyze and understand.
  property("sessionize get results as per simpler alternate algorithm") = forAll(genClientAttribsList, window) { (attrs, w) =>
    val newSeq = attrs.toIndexedSeq
    // results are not defined on empty sequences intentionally
    attrs.length < 1 || (compareResultKeys("bob", newSeq, w) && compareResultValues("bob", newSeq, w))
  }

  def compareResultKeys(client: String,
                        newSeq: IndexedSeq[ClientAttributes],
                        w: Long): Boolean = {
    val real = sessionize(client, newSeq.toIterator, w).toSeq
    val independent = imperativeSessionize(client, newSeq.toIterator, w).toSeq
    val zippedKeys = real.map(x => x.sessionKey).zip(independent.map(_.sessionKey))
    zippedKeys.forall(pair => pair._1 == pair._2) // verify real and independent keys are the same and in same order.
  }

  def compareResultValues(client: String,
                          newSeq: IndexedSeq[ClientAttributes],
                          w: Long): Boolean = {
    val real = sessionize(client, newSeq.toIterator, w).toSeq
    val independent = imperativeSessionize(client, newSeq.toIterator, w).toSeq
    val zippedValues = real.map(x => x.data).zip(independent.map(x => x.data))
    // verify real and independent values are the same and in same order (a bit more complex thanks to Java equality complexity on arrays).
    zippedValues.forall(pair => pair._1.zip(pair._2).forall(pair => pair._1 == pair._2))
  }

  def imperativeSessionize(client: String,
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

   def swap( arr: ArrayBuffer[String], a: Int, b: Int): ArrayBuffer[String] = {
    var buff = arr
    val tmp = buff(a)
    buff(a) = buff(b)
    buff(b) = tmp
    buff
  }

  val tsTokenMove: Gen[Int] = Gen.oneOf(0, 1, 5,6,7) // 0 is correct, 5,6,7 are not
  val clientTokenMove: Gen[Int] = Gen.oneOf(2, 3, 4, 9, 10) // 2 is correct,others are not (note that the bad ones should not overlap)
  val whiteSpace: Gen[String] = Gen.oneOf(" ", "\t")
  val arr = Array("2015-07-22T09:00:28.019143Z", "AWS-Balancer-client", "117.239.195.66:BOGUS_PORT",
    "33333", "44444", "5555", "6666", "7777", "8888", "9999", "10-10-10", "11-11-11", "https://paytm.com:443/api/user/favourite?channel=web&version=2", "13-13-13", "14-14")

  // we build a line randomly from arr tokens above by permuting the timestamp (position 0) and clientIP:port (position 2)
  // and if there's no move, the validation function toClientAccess must return a Some Option
  // but if there's a move, the validation function must return None.
  // We also generate whiteSpace randomly with either a space or tab to separate tokens.
  // Note this solution does not go in full detail of validation, the uri could be ftp://crap.com for instance or even foo-bar.
  property("toClientAccess if timestamp or client tokens are in wrong position fail") =
    forAll(tsTokenMove, clientTokenMove, whiteSpace) { (tsPos, clientPos, ws) =>
      var testArr: ArrayBuffer[String] = ArrayBuffer()
      arr.foreach(x => testArr += x)
      testArr = swap(testArr, 0, tsPos)
      testArr =  swap(testArr, 2, clientPos)
      val line = testArr.mkString(ws)
      val validateLine = toClientAccess(line)
      if(tsPos == 0 && clientPos == 2) validateLine.isDefined
      else validateLine.isEmpty
    }
}
