package spark.apis.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Scala_RDD {

  def main(args: Array[String]) {

    // Initialization:
    val threads = 3 // program simulates a single executor with 3 cores (one local JVM with 3 threads)
    val session = SparkSession.builder.
      master(s"local[$threads]")
      .appName("Scala Word Count RDD")
      .getOrCreate()
    val lines: RDD[String] = session.sparkContext.textFile("src/main/resources/moby_dick.txt")

    // Preprocessing & reducing the input lines:
    val words: RDD[String] = lines.flatMap(_.split("\\s+"))
    val tokenFrequ: RDD[(String, Int)] = words.map(word => (word.toLowerCase(), 1))
    val counts: RDD[(String, Int)] = tokenFrequ.reduceByKey(_ + _)

    // Materializing to local disk:
    counts.coalesce(threads) // optional, without coalesce many tiny output files are generated
      .saveAsTextFile("./countsplits")

    session.stop()
  }
}
