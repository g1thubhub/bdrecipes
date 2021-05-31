package module1.scala.utilities

import java.time.Instant
import java.time.format.DateTimeParseException
import scala.collection.mutable
import module1.scala.utilities.WarcRecord.getCommonFields

/**
 * Domain objects for the web corpus
 *
 * @author Phil, https://github.com/g1thubhub
 */

case class WarcRecord(warcType: String, dateS: Long, recordID: String, contentLength: Int, contentType: String, infoID: String, concurrentTo: String, ip: String, targetURI: String, payloadDigest: String, blockDigest: String, payloadType: String, htmlContentType: String, language: Option[String], htmlLength: Int, htmlSource: String)

object WarcRecord {

  def getCommonFields(metaPairs: mutable.Map[String, String]): (String, String, String, String, String, Long, Int) = {
    val warcType = metaPairs.getOrElse("WARC-Type", "")
    val targetURI = metaPairs.getOrElse("WARC-Target-URI", "")
    val recordID = metaPairs.getOrElse("WARC-Record-ID", "")
    val contentType = metaPairs.getOrElse("Content-Type", "")
    val blockDigest = metaPairs.getOrElse("WARC-Block-Digest", "")
    var dateS = -1L
    var contentLength = -1
    try {
      contentLength = metaPairs.getOrElse("Content-Length", "-1").toInt
      dateS = Instant.parse(metaPairs.getOrElse("WARC-Date", "+1000000000-12-31T23:59:59.999999999Z")).getEpochSecond
    }
    catch {
      case _: NumberFormatException => System.err.println(s"Malformed contentLength field for record $recordID")
      case _: DateTimeParseException => System.err.println(s"Malformed date field for record $recordID")
      case e: Exception => e.printStackTrace()
    }
    (warcType, targetURI, recordID, contentType, blockDigest, dateS, contentLength)
  }

  def apply(metaPairs: mutable.Map[String, String], responseMeta: (String, Option[String], Int), sourceHtml: String): WarcRecord = {
    val (warcType, targetURI, recordID, contentType, blockDigest, dateS, contentLength) = getCommonFields(metaPairs)
    val payloadDigest = metaPairs.getOrElse("WARC-Payload-Digest", "")
    val infoID = metaPairs.getOrElse("WARC-Warcinfo-ID", "")
    val concurrentTo = metaPairs.getOrElse("WARC-Concurrent-To", "")
    val ip = metaPairs.getOrElse("WARC-IP-Address", "")
    val payloadType = metaPairs.getOrElse("WARC-Identified-Payload-Type", "")

    WarcRecord(warcType, dateS, recordID, contentLength, contentType, infoID, concurrentTo, ip, targetURI, payloadDigest, blockDigest, payloadType, responseMeta._1, responseMeta._2, responseMeta._3, sourceHtml)
  }
}

case class WetRecord(warcType: String, targetURI: String, dateS: Long, recordID: String, refersTo: String, blockDigest: String, contentType: String, contentLength: Int, plainText: String)

object WetRecord {
  def apply(metaPairs: mutable.Map[String, String], pageContent: String): WetRecord = {
    val (warcType, targetURI, recordID, contentType, blockDigest, dateS, contentLength) = getCommonFields(metaPairs)
    val refersTo = metaPairs.getOrElse("WARC-Refers-To", "")
    WetRecord(warcType, targetURI, dateS, recordID, refersTo, blockDigest, contentType, contentLength, pageContent)
  }

  import java.util.UUID.randomUUID
  import scala.util.Random
  def createDummy() = WetRecord(randomUUID().toString, randomUUID().toString, System.currentTimeMillis(), randomUUID().toString, randomUUID().toString, randomUUID().toString, randomUUID().toString, Random.nextInt(), randomUUID().toString)

}

