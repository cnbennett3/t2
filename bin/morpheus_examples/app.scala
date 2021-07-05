/**
 * Useful for debugging stuff
 */

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder}
import scala.collection.mutable.WrappedArray
import scala.io.BufferedSource


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
//
//
//var nodeElements: Seq[MorpheusElementTable] = Seq[MorpheusElementTable]()
//var edgeElements: Seq[MorpheusElementTable] = Seq[MorpheusElementTable]()
//val basePath = "/home/yaphet"
//val nodeFileNames = Seq(
//  basePath + "/test_data_t2/Human_GOA_node_file.json",
//  basePath + "/test_data_t2/intact_node_file.json",
//  basePath + "/test_data_t2/Viral_proteome_GOA_node_file.json")
//val edgeFileNames = Seq(
//  basePath + "/test_data_t2/Human_GOA_edge_file.json",
//  basePath + "/test_data_t2/intact_edge_file.json",
//  basePath + "/test_data_t2/Viral_proteome_GOA_edge_file.json")
//
//for (fileName <- nodeFileNames){
//  println("***************************")
//  val start = System.currentTimeMillis()
//  val elements = KGXNodesFileReader.createElementTables(fileName, morpheus)
//  nodeElements = nodeElements ++ elements
//  print("creating element table for"+ fileName + " took: ")
//  print(System.currentTimeMillis() - start )
//  println("(ms)")
//  println("***************************")
//}
//for (fileName <- edgeFileNames){
//  println("***************************")
//  val start = System.currentTimeMillis()
//  val elements = KGXEdgesFileReader.createElementTables(fileName, morpheus)
//  print("creating element table for"+ fileName + " took: ")
//  print(System.currentTimeMillis() - start)
//  edgeElements = edgeElements ++ elements
//  println("(ms)")
//  println("***************************")
//}
//object cypherRunner {
//  def run_cypher_and_show(query: String, graph: RelationalCypherGraph[SparkTable.DataFrameTable]): Unit = {
//    val start = System.currentTimeMillis()
//    val result = graph.cypher(query).records.table.df.show
//    print("Running query: ")
//    println(query)
//    print("took: ")
//    print(System.currentTimeMillis() - start)
//    println(" ms")
//    result
//  }
//}

def getFileAsString(fileUrl: String): String = {
  val bufferedSource: BufferedSource = scala.io.Source.fromURL(fileUrl, "utf-8")
  val content: String = bufferedSource.mkString
  bufferedSource.close()
  content
}
val content =getFileAsString("https://stars.renci.org/var/kgx_data/kegg-node-v0.1.json")
val strippedJson = content.toString.stripLineEnd

val jsonStr = """{ "metadata": { "key": 84896, "value": 54 }}"""




val nodesDF: DataFrame = morpheus.sparkSession.read.option("inferSchema", "true").json(Seq(strippedJson).toDS).toDF
var elementTables = Seq[MorpheusElementTable]()
val nodeTypes = nodesDF.select(col("category")).distinct.collect()
for( nodeType <- nodeTypes) {
  val nodeTypeSeq :Array[String] = nodeType.get(0).asInstanceOf[WrappedArray[String]].toArray[String]
  var filteredNodes = nodesDF.where(nodesDF("category") === nodeTypeSeq)
  // create a new column for internal id
  filteredNodes = filteredNodes.withColumn("_id", filteredNodes.col("id"))
  val node_schema = filteredNodes.schema.filter(_.name != "_id")
  val propKeys = node_schema.map(property =>property.name).toSet[String]
  val nodeMapping: ElementMapping = NodeMappingBuilder.create(
    nodeIdKey = "_id",
    impliedLabels = nodeTypeSeq.toSet,
    propertyKeys = node_schema.map(property => property.name).toSet[String]
  )
  filteredNodes.cache()
  filteredNodes.sort()
  filteredNodes.count()
  val nodeTable: MorpheusElementTable = MorpheusElementTable.create(nodeMapping, filteredNodes)
  elementTables = elementTables ++ Seq(nodeTable)
}
