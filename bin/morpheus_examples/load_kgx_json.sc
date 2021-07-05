import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}

import scala.collection.mutable.WrappedArray
implicit val morpheus: MorpheusSession = MorpheusSession.local()

// Morpheus sesssion
val spark = morpheus.sparkSession

//// node id needs to be long (?)
//// we will map the string id to a new field
//// https://github.com/opencypher/morpheus/blob/a4f2784a216212f566a3329ee672db116dd92803/okapi-api/src/main/scala/org/opencypher/okapi/api/io/conversion/NodeMappingBuilder.scala#L44
//val hashCodify = udf[Int, String](_.hashCode)

val nodeFileName = "/home/yaphet/vp/nodes_small.json"
val edgesFileName = "/home/yaphet/vp/edges_small.json"

// Read json Nodes
var nodesDF: DataFrame = spark.read.format("json").option("inferSchema", "true").load(nodeFileName).toDF

// Create Morpheus node tables from json data frame.
val nodeTypes = nodesDF.select(col("category")).distinct.collect()

// elements table set to contain nodes tables and edge tables
var elementTables = Seq[MorpheusElementTable]()


for( nodeType <- nodeTypes){
  val nodeTypeSeq :Set[String] = nodeType.get(0).asInstanceOf[WrappedArray[String]].toSet[String]
  var filteredNodes = nodesDF.where(nodesDF("category") === nodeType.get(0))
  // create a new column for internal id
  filteredNodes = filteredNodes.withColumn("_id", filteredNodes.col("id"))
  val node_schema = filteredNodes.schema.filter(_.name != "_id")

  val nodeMapping: ElementMapping = NodeMappingBuilder.create(
    nodeIdKey = "_id",
    impliedLabels = nodeTypeSeq,
    propertyKeys = node_schema.map(property => property.name).toSet[String]
  )
  val nodeTable: MorpheusElementTable = MorpheusElementTable.create(nodeMapping, filteredNodes)
  elementTables = elementTables ++ Seq(nodeTable)
}


// -------------- Edges --------------

val edgeDF: DataFrame = spark.read.format("json").option("inferSchema", "true").load(edgesFileName).toDF
val edgeTypes = edgeDF.select(col("edge_label")).distinct.collect()

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
  val edgeTable = MorpheusElementTable.create(relationshipMapping, filtered_edges)
  elementTables = elementTables ++ Seq(edgeTable)
}

// Merge all the elements tables
var graph = morpheus.readFrom(elementTables(0), elementTables.slice(1,elementTables.length): _*)

graph.cache()

//// Test to see if all went well
val result = graph.cypher("Match ()-[e]-() return e limit 10")
result.records.table.df.show()







