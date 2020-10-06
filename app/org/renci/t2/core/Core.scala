package org.renci.t2.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.renci.t2.parser.{KGXEdgesFileReader, KGXNodesFileReader}
import org.renci.t2.util.{KGXMetaData, Version, logger}



class Core(sparkConf: SparkConf , kgxFilesAddress: String) {

  private val morpheusSession: MorpheusSession = this.createMorpheusSession(sparkConf)

  def makeGraph(version: String):RelationalCypherGraph[SparkTable.DataFrameTable] = {
    val kgxServerRoot = this.kgxFilesAddress
    val kgxFilesGrabber: KGXMetaData = new KGXMetaData(kgxServerRoot)
    val versionMetadata: Version = kgxFilesGrabber.getVersionData(version)
    var allNodeTables: Seq[MorpheusElementTable] = Seq[MorpheusElementTable]()
    var allEdgeTables: Seq[MorpheusElementTable] = Seq[MorpheusElementTable]()
    for (nodeFileName <- versionMetadata.nodeFiles) {
      val fileUrl = kgxFilesGrabber.serverRootUrl + "/" + nodeFileName
      logger.debug("Grabbing node file from: " + fileUrl)
      val startTime = System.currentTimeMillis()
      val nodeElementTables = KGXNodesFileReader.createElementTables(fileUrl, this.morpheusSession)
      val timeDiff = System.currentTimeMillis() - startTime
      logger.debug("Converting " + fileUrl + " to morpheus table took : " + timeDiff + " (ms)")
      // Add the node elements for that file into the
      allNodeTables = allNodeTables ++ nodeElementTables
    }
    for (edgeFileName <- versionMetadata.edgeFiles) {
      val fileUrl = kgxFilesGrabber.serverRootUrl + "/" + edgeFileName
      logger.debug("Grabbing node file from: " + fileUrl)
      val startTime = System.currentTimeMillis()
      val edgeElementTables = KGXEdgesFileReader.createElementTables(fileUrl, this.morpheusSession)
      val timeDiff = System.currentTimeMillis() - startTime
      logger.debug("Converting " + fileUrl + " to morpheus table took : " + timeDiff + " (ms)")
      allEdgeTables = allEdgeTables ++ edgeElementTables
    }
    val allElements: Seq[MorpheusElementTable] = allNodeTables ++ allEdgeTables
    val graph = this.morpheusSession.readFrom(allElements(0), allElements.slice(1, allElements.length): _*)
    graph
  }

  def runCypherAndShow(cypherQuery: String, graph: RelationalCypherGraph[SparkTable.DataFrameTable]) : Unit = {
    val start = System.currentTimeMillis()
    graph.cypher(cypherQuery).records.table.df.show
    logger.info("Running query: ")
    logger.info(cypherQuery)
    logger.info("took ")
    logger.info((System.currentTimeMillis() - start).toString)
    logger.info(" ms")
  }

  def runCypherAndReturnJsonString(cypherQuery: String, graph: RelationalCypherGraph[SparkTable.DataFrameTable]) : String = {
    val start = System.currentTimeMillis()
    graph.cypher(cypherQuery).records.table.df.toJSON.collect.mkString("[", "," , "]" )
  }

  def createMorpheusSession(sparkConf: SparkConf): MorpheusSession = {
    val sparkSession: SparkSession = SparkSession.builder
      .config(sparkConf)
      .getOrCreate()
    val morpheusSession: MorpheusSession = MorpheusSession.create(sparkSession)
    morpheusSession
  }

}
