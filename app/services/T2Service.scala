package services

import javax.inject.{Inject, Singleton}
import org.apache.spark.SparkConf
import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.renci.t2.core.Core
import play.api.Configuration

@Singleton
class T2Service @Inject() (config: Configuration) {
  val core: Core = this.initializeT2Core()
  val graph: RelationalCypherGraph[SparkTable.DataFrameTable] = this.core.makeGraph(config.get[String]("t2.kgx.version"))
  def initializeT2Core(): Core = {
    val sparkConf: SparkConf = new SparkConf()
    // Set some configs
    sparkConf.setMaster(
      config.get[String]("t2.spark.master")
    ).setAppName(
      config.get[String]("t2.spark.appName")
    ).set(
      "spark.executor.memory", config.get[String]("t2.spark.executor.memory")
//    ).set(
//      "spark.executor.instances", config.get[String]("t2.spark.executor.instances")
//    ).set(
//      "spark.kubernetes.container.image", config.get[String]("t2.spark.kubernetes.container.image")
    )
    // Voila we are set make core
    val kgxFileServer = config.get[String]("t2.kgx.serverRoot")
    new Core(sparkConf, kgxFileServer)
  }

  def runCypher(cypher: String): String = {
    this.core.runCypherAndReturnJsonString(cypher, this.graph)
  }



}