import org.apache.spark.sql.{DataFrame, Row, Dataset}
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable, MorpheusElementTable}
import scala.collection.mutable.ListBuffer
import spark.sqlContext.implicits._
////
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import collection.{breakOut, immutable}
import sys.process._
import scala.io.Source
import scala.language.postfixOps

// Java imports for File I/O:
import java.io.PrintWriter
import java.io.{BufferedWriter, File, FileWriter}


// Morpheus: https://github.com/opencypher/morpheus
// Hygenic Scala Closures: http://erikerlandson.github.io/blog/2015/03/31/hygienic-closures-for-scala-function-serialization/
// Passing sequence to varargs: https://stackoverflow.com/questions/1832061/scala-pass-seq-to-var-args-functions


println ("Define node and edge case classes.")
case class Edge(
     id:String, source:String, target:String, relation:String, predicate_id:String, relation_label:String, edge_type:String)

// CHUCK: START HERE:
// create a a case class for gene and a class for disease here . . .
case class Node(id:String, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, node_type:String)


//-----------------------------------------------------------------
// Gene
//-----------------------------------------------------------------
println("Create edges df with desired columns from edges.csv.")
val initialEdgesDF = spark.read.format("csv").option("header", "true").option("quote", "\"").option("escape", "\"").load("target/edges2.csv")
val edgesDFMin = initialEdgesDF.select("subject", "object", "relation", "predicate_id", "relation_label")

// Add 64-bit integer for Morpheus
var edgesDfUc = edgesDFMin.withColumn("id", row_number().over(Window.orderBy("subject")))
val edgesColumns: Array[String] = edgesDfUc.columns

val edgesDFTmp = edgesDfUc.withColumn("edge_type", lit("gene")) // Add edge_type
val newEdgesColOrder = Array("id", "subject", "object", "relation", "predicate_id", "relation_label", "edge_type")
val edgesDF_U = edgesDFTmp.select(newEdgesColOrder.head, newEdgesColOrder.tail: _*)

// Rename object col for Scala
val edgesDF = edgesDF_U.withColumnRenamed("subject", "source").withColumnRenamed("object",  "target")


println("Create nodes df with desired columns from nodes.csv")
val initialNodesDF = spark.read.format("csv").option("header", "true").option("quote", "\"").option("escape", "\"").load("target/nodes2.csv")
val nodesDFTmp = initialNodesDF.select("id", "category", "name", "equivalent_identifiers")
val nodesDFTmp2 = nodesDFTmp.withColumnRenamed("id","curie_or_id")
val nodesDFProto_U = nodesDFTmp2.withColumn("node_type", lit("gene")) // Add node_type



println("Add 64-bit unique (within nodes) identifier to nodes dataframe")
var nodesDfUc = nodesDFProto_U.withColumn("id", row_number().over(Window.orderBy("category")))
val columns: Array[String] = nodesDfUc.columns
val newColOrder = Array("id", "category", "curie_or_id", "name", "equivalent_identifiers", "node_type")
val nodesDF = nodesDfUc.select(newColOrder.head, newColOrder.tail: _*)


nodesDF.show(30)
edgesDF.show(125)


println ("show edge and node schemas.")
edgesDF.schema.printTreeString
nodesDF.schema.printTreeString


// Select gene part and create a gene-only dataframe
val geneDF:DataFrame = nodesDF.filter($"category" === "named_thing|gene")

// Select disease part and create a disease-only dataframe
val diseaseDF:DataFrame = nodesDF.filter($"category" === "named_thing|disease")


println ("Define graph loader")
object GraphLoader {
  // Parse string arrays from text.
  def stringToStringList(string: String) : List[String] = {
    println("stringToStringList:" + string)
    string.drop(1).dropRight(1).split(",").map (v => v.trim.drop(1).dropRight(1)).toList
  }
  // Load edges grouped by label

  // edgesDFsubset line  gets relation_label field and uses it as the relType in MorpheusRelationshipTable call

  val loadEdges = (relationLabel:String, edges:DataFrame, relationshipBuffer:ListBuffer[MorpheusElementTable]) => {
    var labels = stringToStringList (relationLabel)
    labels.foreach { label =>
      var edgesDFsubset = edges.filter (e => e.getAs[String]("relation_label") == label)
      relationshipBuffer += MorpheusRelationshipTable(
        label,
        edgesDFsubset.toDF ("id", "source", "target", "relation", "predicate_id", "relation_label", "edge_type")) // should this be MorhpeusElementTable????
    }
  }
  // Filter edges by label
  val filterEdges = (label:String, edgeDF:Dataset[Edge]) => edgesDF.filter ("relation_label == label")
  // Query the graph
  val query = (nodesDF:DataFrame, relationshipBuffer:ListBuffer[MorpheusElementTable]) => {
    println ("Build a node table for the supported node type.")
    println("RELATIONSHIPBUFFER:" + relationshipBuffer)
    var nodeTable = MorpheusNodeTable(Set("Gene"), nodesDF.toDF)  // should this be  Set("Node") ????

    println ("Initialize Morpheus...")
    implicit val morpheus: MorpheusSession = MorpheusSession.local()
    val spark = morpheus.sparkSession

    println ("Create the graph.") // Passing sequence to varargs: https://stackoverflow.com/questions/1832061/scala-pass-seq-to-var-args-functions
    val graph = morpheus.readFrom(nodeTable, relationshipBuffer.toSeq:_*)

    println ("Execute Cypher query and print results")
    val result = graph.cypher("MATCH (n:Gene) RETURN n.name, n.id")

    println ("Collect results into string by selecting a specific column: name")
    // This operation may be very expensive as it materializes results locally.
    //val names: Set[String] = result.records.table.df.collect().map(_.getAs[String]("name")).toSet
    val names: Set[String] = result.records.table.df.collect().map(_.getAs[String]("n_name")).toSet
    println(names)

    println ("Collect results into string by selecting a specific column: id")
    val ids: Set[String] = result.records.table.df.collect().map(_.getAs[String]("n_id")).toSet
    println(ids)

    println ("Execute Cypher query and print results")
    val result2 = graph.cypher("MATCH (n:Gene)--(m) RETURN m")

    println ("Collect results into string by selecting a specific column: name")
    // This operation may be very expensive as it materializes results locally.
    //val names: Set[String] = result.records.table.df.collect().map(_.getAs[String]("name")).toSet
    val names2: Set[String] = result2.records.table.df.collect().map(_.getAs[String]("m_name")).toSet
    println(names2)


  }
}

// Build a buffer of relationship tables.
var relationshipBuffer = ListBuffer[MorpheusElementTable] ()
var edge_labels = edgesDF.map (e => e.getAs[String]("relation_label")).distinct ().collect ()

println("edge_labels:")
println(edge_labels)

edge_labels.foreach { edge_label =>
  GraphLoader.loadEdges (edge_label, edgesDF.toDF (), relationshipBuffer)
}
GraphLoader.query (nodesDF.toDF, relationshipBuffer)


println ("Initialize Morpheus...")
implicit val morpheus: MorpheusSession = MorpheusSession.local()
val spark = morpheus.sparkSession


println("========================================================================================")
println("                                Test")
println("========================================================================================")


val geneTable = MorpheusNodeTable(Set("X"), geneDF)
val diseaseTable = MorpheusNodeTable(Set("Y"), diseaseDF)


//val geneDiseaseTable = MorpheusNodeTable(Set("Node"), nodesDF.toDF())
val geneToDiseaseRelationshipTable = MorpheusRelationshipTable("RELATION", edgesDF.toDF())

val graph = morpheus.readFrom(geneTable, diseaseTable, geneToDiseaseRelationshipTable)
//val graph = morpheus.readFrom(geneDiseaseTable, geneToDiseaseRelationshipTable)

// Execute query and cross fingers

//val finalResult = graph.cypher("MATCH (g:Gene {name: 'TYR'})--(d:Disease) RETURN d")
val finalResult = graph.cypher("MATCH (g)--(d) RETURN d")

finalResult.records.show






/*

println("relationshipBuffer:")
println(relationshipBuffer)

println ("Filter by label")
//val filterEdges = (label:String, edgeDF:Dataset[Edge]) => edgesDF.filter ("relation_label == label")
val filterEdges = (label:String, edgeDF:Dataset[Row]) => edgesDF.filter (e => e.getAs[String]("relation_label") == label)
println("filterEdges (relation_label):")
println(filterEdges)

//------------------------------------------------------------------------------------------------------
//------------------------------------------------------------------------------------------------------
// In here, we need to map the relationship (ie, relation or relation_label in edgesDF to the relType 
// of the MorpheusRelationshipTable.  I think this can be done explicitly using the .relType method,
// or using the first parameter of the MorpheusRelationShipTable function.
//------------------------------------------------------------------------------------------------------
//------------------------------------------------------------------------------------------------------

var label : String = "causes condition"
println (filterEdges(label, edgesDF).collect ())

println("Create MorpheusRelationshipTable relsTable from filter edges.")
var relsTable = MorpheusRelationshipTable(
  label,
  // edgesDF.filter (e => e.relation_label == label).toDF (
  filterEdges (label, edgesDF).toDF (
    "id", "source", "target", "relation", "predicate_id", "relation_label", "edge_type"))

println ("Build a node table for the supported node type.")
//var nodeTable = MorpheusNodeTable(Set("Gene"), nodesDF.toDF)
var nodeTable = MorpheusNodeTable(Set("Node"), nodesDF.toDF)


println ("Create Morpheus graph from nodes and edges.")
val graph = morpheus.readFrom(nodeTable, relsTable)

// ----------------------------------------------------------------------
// TODO: Come up with a real query from disease to gene and print result.
// ----------------------------------------------------------------------
println ("Execute Cypher query and print results")
println ("Query 1: Single ended query: MATCH (n:Gene) RETURN n.name")
val result = graph.cypher("MATCH (n:Gene) RETURN n.n_name")
val names: Set[String] = result.records.table.df.take(10).map(_.getAs[String]("n_name")).toSet
println(names)

println ("\nQuery 2: 2 Node query: MATCH (g:Gene)--(d) WHERE g.name = 'TYR'  RETURN d")
val geneToDiseaseResult = graph.cypher("MATCH (g:Gene {name: 'TYR'})--(d)  RETURN d")
val diseaseNames: Set[String] = geneToDiseaseResult.records.table.df.collect().map(_.getAs[String]("d_name")).toSet
println(diseaseNames)

println ("\nQuery 3: 2 Node query: MATCH (g:Gene {name: 'TYR'})--(d:Disease {name: 'disease of orbital region'})  RETURN d")
val geneToDiseaseResult2 = graph.cypher("MATCH (g:Gene {name: 'TYR'})--(d:Gene2 {name: 'disease of orbital region'}) RETURN d")
val diseaseNames2: Set[String] = geneToDiseaseResult2.records.table.df.collect().map(_.getAs[String]("d_name")).toSet
println(diseaseNames2)
*/
println ("Done.")

