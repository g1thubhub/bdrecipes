package module1.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}
import module1.scala.utilities.WarcRecord
import module1.scala.utilities.HelperScala.{createSession, extractRawRecords, parseRawWarc}

object QueryPlans {
  def main(args: Array[String]): Unit = {
    val sampleLocation = if (args.nonEmpty) args(0) else "/Users/me/IdeaProjects/bdrecipes/resources/warc.sample" // ToDo: Modify path
    implicit val spark: SparkSession = createSession(3, "Query Plans")
    spark.sparkContext.setLogLevel("TRACE")
    import spark.implicits._

    val langTagMapping = Seq[(String, String)](("en", "english"), ("pt-pt", "portugese"), ("cs", "czech"), ("de", "german"), ("es", "spanish"), ("eu", "basque"), ("it", "italian"), ("hu", "hungarian"), ("pt-br", "portugese"), ("fr", "french"), ("en-US", "english"), ("zh-TW", "chinese"))
    val langTagDF: DataFrame = langTagMapping.toDF("tag", "language")

    val warcRecordsRdd: RDD[WarcRecord] = extractRawRecords(sampleLocation).flatMap(parseRawWarc)
    val warcRecordsDf: DataFrame = warcRecordsRdd.toDF()
      .select('targetURI, 'language)
      .filter('language.isNotNull)

    val aggregated = warcRecordsDf
      .groupBy('language)
      .agg(count('targetURI))
      .withColumnRenamed("language", "tag")

    val joinedDf: DataFrame = aggregated.join(langTagDF, Seq("tag"))

    joinedDf.show()
    joinedDf.explain(true)
    Thread.sleep(10L * 60L * 1000L) // Freeze for 10 minutes
  }
}