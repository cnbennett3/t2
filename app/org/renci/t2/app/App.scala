package org.renci.t2.app

import org.apache.spark.SparkConf
import org.renci.t2.core.Core


object App {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster(
      "spark://localhost:7077"
    ).setAppName(
      "t2-cli"
    ).set(
      "spark.executor.memory", "4g"
    )
    val core:Core = new Core(sparkConf, "https://stars.renci.org/var/kgx_data")
    val graph = core.makeGraph("v0.1")
    core.runCypherAndShow(cypherQuery = "MATCH (c) return count(c)", graph = graph)

  }

}
