package org.renci.t2.parser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder}

import scala.collection.mutable.WrappedArray

object KGXNodesFileReader extends KGXFileReader {

  override def createElementTables(filepath: String, session: MorpheusSession): Seq[MorpheusElementTable] = {
    /***
     * Reads kgx nodes files and converts them to to MorpheusElements table
     */
    val nodesDF: DataFrame = this.convertKGXFileToDataFrame(filePath=filepath, session=session)
    var elementTables = Seq[MorpheusElementTable]()
    val nodeTypes = nodesDF.select(col("category")).distinct.collect()
    for( nodeType <- nodeTypes){
      val nodeTypeSeq :Array[String] = nodeType.get(0).asInstanceOf[WrappedArray[String]].toArray[String]
      var filteredNodes = nodesDF.where(nodesDF("category") === nodeTypeSeq)
      // create a new column for internal id
      filteredNodes = filteredNodes.withColumn("_id", filteredNodes.col("id"))
      val node_schema = filteredNodes.schema.filter(_.name != "_id")
      val nodeMapping: ElementMapping = NodeMappingBuilder.create(
        nodeIdKey = "_id",
        impliedLabels = nodeTypeSeq.toSet,
        propertyKeys = node_schema.map(property => property.name).toSet[String]
      )
      filteredNodes.cache()
      filteredNodes.checkpoint(true)
      filteredNodes.sort()
      filteredNodes.count()
      val nodeTable: MorpheusElementTable = MorpheusElementTable.create(nodeMapping, filteredNodes)
      elementTables = elementTables ++ Seq(nodeTable)
    }
    session.sparkSession.sparkContext.broadcast(elementTables)

    elementTables
  }
}
