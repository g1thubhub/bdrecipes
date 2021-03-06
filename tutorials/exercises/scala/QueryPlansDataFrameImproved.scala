package exercises.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import module1.scala.utilities.{WarcRecord, WetRecord}
import module1.scala.utilities.HelperScala.{createSession, extractRawRecords, parseRawWarc, parseRawWet}

object QueryPlansDataFrameImproved {
  def main(args: Array[String]): Unit = {
    val inputLocWarc = if (args.nonEmpty) args(0) else "/Users/me/IdeaProjects/bdrecipes/resources/CC-MAIN-20191013195541-20191013222541-00000.warc" // ToDo: Modify path
    val inputLocWet = if (args.nonEmpty) args(1) else "/Users/me/IdeaProjects/bdrecipes/resources/CC-MAIN-20191013195541-20191013222541-00000.warc.wet" // ToDo: Modify path
    implicit val spark: SparkSession = createSession(3, "Hermeneutics Exercise DataFrame Improved")

    val warcRecords: RDD[WarcRecord] = extractRawRecords(inputLocWarc).flatMap(parseRawWarc)
    val wetRecords: RDD[WetRecord] = extractRawRecords(inputLocWet).flatMap(parseRawWet)

    import spark.implicits._
    val warcRecordsDf: DataFrame = warcRecords.toDF.select("targetURI", "language")
      .filter('language === "es")
    val wetRecordsDf: DataFrame = wetRecords.toDF.select("targetURI", "plainText")

    val joinedDf = warcRecordsDf.join(wetRecordsDf, Seq("targetURI"))
    println(s"@@ Result: ${joinedDf.count()}") // 133
    Thread.sleep(10L * 60L * 1000L) // For exploring WebUI
  }
}
