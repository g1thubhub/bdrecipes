package module1.scala.catalyst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{SparkPlanner, SparkStrategy}

object CollectStrategies {
  def main(args: Array[String]): Unit = {
    // Creating a SparkSession object
    val session = SparkSession.builder
      .master("local[2]")
      .appName("Exploring Spark Planner strategies")
      .getOrCreate()

    // when using Spark 2.X:
    // val myPlanner = new SparkPlanner(session.sparkContext, session.sessionState.conf, session.sessionState.experimentalMethods)
    val myPlanner = new SparkPlanner(session, session.sessionState.experimentalMethods)
    val strategies: Seq[SparkStrategy] = myPlanner.strategies
    strategies.foreach(println)
  }

}
