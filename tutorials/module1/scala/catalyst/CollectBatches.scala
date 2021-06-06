package module1.scala.catalyst

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, SimpleAnalyzer}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.internal.SQLConf

object CollectBatches {

  def main(args: Array[String]): Unit = {
    // Creating a SparkSession object
    val session = SparkSession.builder
      .master("local[2]")
      .appName("Exploring Spark Optimizer")
      .getOrCreate()

    // helper objects needed for creating the optimizer
    val sqlConf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> true)
    val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, sqlConf)
    // val myOptimizer = new SparkOptimizer(catalog, session.experimental) // when using Spark 2.X
    val catalogManager = SimpleAnalyzer.catalogManager // CatalogManager itself has private visibility in Spark 3

    // Creating a Spark optimizer object and collecting batches
    val myOptimizer = new SparkOptimizer(catalogManager, catalog, session.experimental)
    val batchesInfo: Seq[(String, Seq[Rule[LogicalPlan]], myOptimizer.Strategy)] = myOptimizer.batches.map(batch => {
      (batch.name, batch.rules, batch.strategy)
    })

    // print out batch info per rule
    batchesInfo.foreach(batch => {
      val batchName = batch._1
      val strategyName = batch._3.toString
      val ruleNamesOfBatch = batch._2.map(_.ruleName)
      ruleNamesOfBatch.foreach(rule => println(s"$batchName\t$rule\t$strategyName"))
    })

    // For individual batches & rules
    val batchNames: Seq[String] = myOptimizer.batches.map(_.name)
    val allRuleNames: Seq[String] = myOptimizer.batches.flatMap(_.rules).map(_.ruleName)
    /*
      batchNames.foreach(println)
      allRuleNames.distinct.foreach(println)
     */
  }

}