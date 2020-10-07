package services

import javax.inject.Inject
import org.apache.spark.SparkConf
import org.renci.t2.core.Core
import play.api.Configuration

class T2Service @Inject() (config: Configuration) {
  def initializeT2Core(): Core = {
    val sparkConf: SparkConf = new SparkConf()
    // Set some configs
    sparkConf.setMaster(
      config.get[String]("t2.spark.master")
    ).setAppName(
      config.get[String]("t2.spark.appName")
    ).set(
      "spark.executor.memory", config.get[String]("t2.spark.executor.memory")
    )
    // Voila we are set make core
    val kgxFileServer = config.get[String]("t2.kgx.serverRoot")
    new Core(sparkConf, kgxFileServer)
  }
}