import com.nvidia.spark.rapids.{RapidsDriverPlugin, RapidsExecutorPlugin}

object EntryPoint extends App {
  val driverPlugin = new RapidsDriverPlugin()
  val executorPlugin = new RapidsExecutorPlugin()

  /*
  https://nvidia.github.io/spark-rapids/developer-overview/
  How the RAPIDS Plugin Works
  The plugin leverages two main features in Spark. The first is a plugin interface in Catalyst that allows the optimizer
  to be extended. The plugin is a Catalyst extension that analyzes the physical plan and replaces executor and expression
  nodes with GPU versions when those operations can be performed on the GPU. The other feature is columnar processing
  which allows extensions to operate on Spark SQL data in a ColumnarBatch form. Processing columnar data is much more GPU
  friendly than row-by-row processing.

  The plugin uses a set of rules to update the query plan. The physical plan is walked, node by node, looking up rules based
  on the type of node (e.g.: scan, executor, expression, etc.), and applying the rule that matches.
  See the ColumnarOverrideRules and GpuOverrides classes for more details.
  */


}
