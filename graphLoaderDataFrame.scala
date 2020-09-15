import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.api.table.Table

import scala.collection.mutable.WrappedArray

trait KGXFileReader{
  /**
   *  Creates morpheus element table
   */
  def convertKGXFileToDataFrame(filePath: String, session: MorpheusSession): DataFrame = {
    /**
     * Creates a dataframe from a json formatted kgx file
     */

    session.sparkSession.read.format("json").option("inferSchema", "true").load(filePath).toDF
  }

  def createElementTables(filePath: String, session:MorpheusSession): Seq[MorpheusElementTable]
}


object KGXNodesFileReader extends KGXFileReader {

  override def createElementTables(filepath: String, session: MorpheusSession): Seq[MorpheusElementTable] = {
    /***
     * Reads kgx nodes files and converts them to to MorpheusElements table
     */
    val nodesDF: DataFrame = this.convertKGXFileToDataFrame(filePath=filepath, session=session)
    var elementTables = Seq[MorpheusElementTable]()
    val nodeTypes = nodesDF.select(col("category")).distinct.collect()
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
      nodeTable.cache()
      elementTables = elementTables ++ Seq(nodeTable)
    }
    elementTables
  }
}

object KGXEdgesFileReader extends KGXFileReader {
  override def createElementTables(filePath: String, session: MorpheusSession): Seq[MorpheusElementTable] = {
    /**
     *
     */
    val edgeDF: DataFrame = session.sparkSession
      .read
      .format("json")
      .option("inferSchema", "true")
      .load(filePath)
      .toDF
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
      val edgeTable = MorpheusElementTable.create(relationshipMapping, filtered_edges)
      edgeTable.cache()
      elementTables = elementTables ++ Seq(edgeTable)
    }
    // Give back element tables
    elementTables
  }
}

implicit val morpheus: MorpheusSession = MorpheusSession.local()
// Morpheus sesssion

//val nodeFileName = "./test_small_files/nodes_small.json"
//val edgesFileName = "./test_small_files/edges_small.json"
//
//val nodeElements = KGXNodesFileReader.createElementTables(nodeFileName, morpheus)
//val edgeElements = KGXEdgesFileReader.createElementTables(edgesFileName, morpheus)
//
//val allElements: Seq[MorpheusElementTable] = nodeElements ++ edgeElements
//val graph2= morpheus.readFrom(allElements(0), allElements.slice(1, allElements.length): _*)
//graph2.cache()
////// Test to see if all went well
//val result = graph2.cypher("Match ()-[e]-() return e limit 10")
//result.records.table.df.show()


var nodeElements: Seq[MorpheusElementTable] = Seq[MorpheusElementTable]()
var edgeElements: Seq[MorpheusElementTable] = Seq[MorpheusElementTable]()
val basePath = "/home/yaphet"
val nodeFileNames = Seq(
  basePath + "/test_data_t2/Human_GOA_node_file.json",
  basePath + "/test_data_t2/intact_node_file.json",
  basePath + "/test_data_t2/Viral_proteome_GOA_node_file.json")
val edgeFileNames = Seq(
  basePath + "/test_data_t2/Human_GOA_edge_file.json",
  basePath + "/test_data_t2/intact_edge_file.json",
  basePath + "/test_data_t2/Viral_proteome_GOA_edge_file.json")

for (fileName <- nodeFileNames){
  println("***************************")
  val start = System.currentTimeMillis()
  val elements = KGXNodesFileReader.createElementTables(fileName, morpheus)
  nodeElements = nodeElements ++ elements
  print("creating element table for"+ fileName + " took: ")
  print(System.currentTimeMillis() - start )
  println("(ms)")
  println("***************************")
}
for (fileName <- edgeFileNames){
  println("***************************")
  val start = System.currentTimeMillis()
  val elements = KGXEdgesFileReader.createElementTables(fileName, morpheus)
  print("creating element table for"+ fileName + " took: ")
  print(System.currentTimeMillis() - start)
  edgeElements = edgeElements ++ elements
  println("(ms)")
  println("***************************")
}
object cypherRunner {
  def run_cypher_and_show(query: String, graph: RelationalCypherGraph[SparkTable.DataFrameTable]): Unit = {
    val start = System.currentTimeMillis()
    val result = graph.cypher(query).records.table.df.show
    print("Running query: ")
    println(query)
    print("took")
    print(System.currentTimeMillis() - start)
    println("ms")
    result
  }
}


val allElementTables = nodeElements ++ edgeElements

val graph = morpheus.readFrom(allElementTables(0), allElementTables.slice(1, allElementTables.length): _*)

