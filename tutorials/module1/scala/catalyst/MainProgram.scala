package module1.scala.catalyst

import java.io.File
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainProgram {

  def main(args: Array[String]): Unit = {
    // ToDo: The following path name points to the resources/catalyst_input folder, might need to be modified
    // csv files were generated with `GenerateCsvFiles.scala` and are included under resources/catalyst_input
    val inputPathName = "src/main/resources/catalyst_input"
    val inputPath: String = new File(inputPathName)
      .getAbsolutePath

    val session = SparkSession.builder // Create SparkSession object
      .master("local[2]")
      .appName("Example for exploring Catalyst")
      .getOrCreate()
    session.sparkContext.setLogLevel("TRACE")
    import session.implicits._

    // Read from .csv files and process records
    val result: DataFrame = session.read.option("header", "true").csv(inputPath)
      .select("letter", "index")
      .filter($"index" < 10)
      .filter(!$"letter".isin("h") && $"index" > 6)
    result.show()

    /* Output:
      +------+-----+
      |letter|index|
      +------+-----+
      |     i|    8|
      |     j|    9|
      +------+-----+
    */
  }

}