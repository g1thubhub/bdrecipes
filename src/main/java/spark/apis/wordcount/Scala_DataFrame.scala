package spark.apis.wordcount

import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, functions}

object Scala_DataFrame {

  def main(args: Array[String]) {

    // Initialization:
    val threads = 3 // program simulates a single executor with 3 cores (one local JVM with 3 threads)
    val session = SparkSession.builder.
      master(s"local[$threads]")
      .appName("Scala Word Count Dataframe")
      .getOrCreate()
    val lines: sql.DataFrame = session.sqlContext.read.text("src/main/resources/moby_dick.txt")

    // preprocessing & reducing the input lines:
    val words: sql.DataFrame = lines.select(functions.explode(functions.split(lines("value"), "\\s+")))
    val tokens: sql.DataFrame = words.where(functions.length(words("col")) > 0) // filter empty strings
      .select(functions.lower(words("col")) // lowercase strings
      .alias("token"))
    val counts: sql.DataFrame = tokens.groupBy("token").count()

    // materializing to local disk:
    counts.coalesce(threads)  // optional, without coalesce many tiny output files are generated
      .write.csv("./countsplits")

    session.stop()

  }
}

