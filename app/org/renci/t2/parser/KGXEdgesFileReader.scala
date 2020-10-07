package org.renci.t2.parser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.okapi.api.io.conversion.{ElementMapping, RelationshipMappingBuilder}

object KGXEdgesFileReader extends KGXFileReader {
  override def createElementTables(filePath: String, session: MorpheusSession): Seq[MorpheusElementTable] = {
    /**
     *
     */
    val edgeDF: DataFrame = this.convertKGXFileToDataFrame(filePath, session=session)
    val edgeTypes = edgeDF.select(col("edge_label")).distinct.collect()
    var elementTables = Seq[MorpheusElementTable]()
    for (edgeType <- edgeTypes) {
      val edgeTypeStr: String = edgeType.get(0).asInstanceOf[String]
      var filtered_edges = edgeDF.filter(edgeDF.col("edge_label") === edgeTypeStr)
      val edgesTableSchema = filtered_edges.schema

      // Morpheus converts source target and id keys to Long type.
      // To avoid conversion of the original we will dup these columns.
      filtered_edges = filtered_edges
        .withColumn("_source_id", filtered_edges.col("subject"))
        .withColumn("_target_id", filtered_edges.col("object"))
        .withColumn("_id", filtered_edges.col("id"))

      //
      val relationshipMapping: ElementMapping = RelationshipMappingBuilder.create(
        sourceIdKey = "_id",
        sourceStartNodeKey = "_source_id",
        sourceEndNodeKey = "_target_id",
        relType = edgeTypeStr,
        properties = edgesTableSchema.map(property => property.name).toSet[String]
      )
      filtered_edges.cache()
      filtered_edges.sort()
      filtered_edges.count()
      val edgeTable = MorpheusElementTable.create(relationshipMapping, filtered_edges)

      elementTables = elementTables ++ Seq(edgeTable)
    }

    // Give back element tables
    elementTables
  }
}