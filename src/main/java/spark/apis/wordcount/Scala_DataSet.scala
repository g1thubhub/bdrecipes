package spark.apis.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

object Scala_DataSet {

  def main(args: Array[String]) {

    // Initialization:
    val threads = 3 // program simulates a single executor with 3 cores (one local JVM with 3 threads)
    val session = SparkSession.builder.
      master(s"local[$threads]")
      .appName("Scala Word Count DataSet")
      .getOrCreate()
    import session.implicits._ // needed for functions below that require implicit encoders
    val lines: Dataset[String] = session.read.text("src/main/resources/moby_dick.txt").as[String]

    // Preprocessing & reducing the input lines:
    val tokens: Dataset[String] = lines.flatMap(_.split("\\s+"))
    val groupedTokens = tokens.groupByKey(_.toLowerCase)
    val counts: Dataset[(String, Long)] = groupedTokens.count()

    // Materializing to local disk:
    counts.coalesce(threads) // optional, without coalesce many tiny output files are generated
      .write.csv("./countsplits")

    session.stop()

  }
}
