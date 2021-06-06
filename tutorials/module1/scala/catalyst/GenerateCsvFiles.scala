package module1.scala.catalyst

import org.apache.spark.sql.{DataFrame, SparkSession}

object GenerateCsvFiles {

  val outputLocation: String = ??? // ToDo: Specify output path for csv files

  def main(args: Array[String]): Unit = {
    val lettersWithIndex = ('a' to 'z')
      .map(_.toString)
      .zipWithIndex
      .map(pair => (pair._1, pair._1.toUpperCase, pair._2))

    // Create SparkSession
    val session = SparkSession.builder
      .master("local[2]")
      .appName("Creating the csv files")
      .getOrCreate()
    import session.implicits._

    // Create DataFrame object and write records to disk
    val charsWithIndexDf: DataFrame = lettersWithIndex
      .toDF("letter", "letterUppercase", "index")
    charsWithIndexDf
      .write
      .option("header", "true")
      .csv(outputLocation)
  }

}