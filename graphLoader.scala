import org.apache.spark.sql.{DataFrame, Row, Dataset}
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable, MorpheusElementTable}
import scala.collection.mutable.ListBuffer
import spark.sqlContext.implicits._
////
import org.apache.spark.sql.functions._
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
  id:String, subject:String, obj:String, relation:String, relation_label:String, predicate_id:String)

case class Node(id:String, name:String, equivalent_identifiers:String)


println("Create edges.csv.min with column headers for edges df.")
val edgesInFile  = "target/edges.csv"
val edgesOutFile = "target/edges.csv.min"
val firstCmd     = "head -1 " + edgesInFile + " "
val secondCmd    = "cut -d, -f 1-4,11,9 "
val edgerc = firstCmd  #| secondCmd #> new File(edgesOutFile)!


println("Select desired columns from nodes.csv for nodes df.")
var nodes = spark.read.format("csv").option("header", "true").load("target/nodes.csv")
var selectedNodes = nodes.select("id", "name", "equivalent_identifiers")
selectedNodes.coalesce(1).write.format("csv").option("header", "true").option("quote", "").save("target/nodes.csv.tmp")
val cmdRc1 = Seq("/bin/sh", "-c", "cat target/nodes.csv.tmp/part-*.csv") #| "tr -cd '\u0001-\u0021,\u0023-\u007f'" #> new File("target/nodes.csv.min")!


println ("Import the KGX exports into data frames of our Edge and Node types.")
//var edgesDF = spark.read.format("csv").option("header", "true").load(userDir + "/target/edges.csv.min").withColumnRenamed("object", "obj").as[Edge]
//var nodesDFProto = spark.read.format("csv").option("header", "true").load(userDir + "/target/nodes.csv.min").as[Node]
var edgesDF = spark.read.format("csv").option("header", "true").load("target/edges.csv.min").withColumnRenamed("object", "obj").as[Edge]
var nodesDFProto = spark.read.format("csv").option("header", "true").load("target/nodes.csv.min").as[Node]


println ("Create nodes to act as the endpoint for the edges we have and union these to our other nodes.")
var otherNodesDF = edgesDF.map ({ edge =>
  Node(id=edge.obj, name=edge.obj, equivalent_identifiers="..equivids..")
}).distinct ()
var nodesDF = nodesDFProto.union (otherNodesDF)

println ("show edge and node schemas.")
edgesDF.schema.printTreeString
nodesDF.schema.printTreeString

println ("Define graph loader")
object GraphLoader {
  // Parse string arrays from text.
  def stringToStringList(string: String) : List[String] = {
    string.drop(1).dropRight(1).split(",").map (v => v.trim.drop(1).dropRight(1)).toList
  }
  // Load edges grouped by label
  val loadEdges = (relationLabel:String, edges:DataFrame, relationshipBuffer:ListBuffer[MorpheusElementTable]) => {
    var labels = stringToStringList (relationLabel)
    labels.foreach { label =>
      print (s"   processing label: {label}")
      var edgesDFsubset = edges.filter (e => e.getString(4) == label)
      relationshipBuffer += MorpheusRelationshipTable(
        label,
        edgesDFsubset.toDF ("id", "source", "target", "relation", "relation_label", "predicate_id"))
    }
  }
  // Filter edges by label
  val filterEdges = (label:String, edgeDF:Dataset[Edge]) => edgesDF.filter (e => e.relation_label == label)
  // Query the graph
  val query = (nodesDF:DataFrame, relationshipBuffer:ListBuffer[MorpheusElementTable]) => {
    println ("Build a node table for the supported node type.")
    var nodeTable = MorpheusNodeTable(Set("Gene"), nodesDF.toDF)

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
    val result2 = graph.cypher("MATCH (n:Gene)--(m) RETURN n.name, n.id, m.name")

    println ("Collect results into string by selecting a specific column: name")
    // This operation may be very expensive as it materializes results locally.
    //val names: Set[String] = result.records.table.df.collect().map(_.getAs[String]("name")).toSet
    val names2: Set[String] = result2.records.table.df.collect().map(_.getAs[String]("m_name")).toSet
    println(names2)


  }
}


// Build a buffer of relationship tables.
var relationshipBuffer = ListBuffer[MorpheusElementTable] ()
var edge_labels = edgesDF.map (e => e.relation_label).distinct ().collect ()
edge_labels.foreach { edge_label =>
  GraphLoader.loadEdges (edge_label, edgesDF.toDF (), relationshipBuffer)
}
GraphLoader.query (nodesDF.toDF, relationshipBuffer)


println ("Filter by label")
val filterEdges = (label:String, edgeDF:Dataset[Edge]) => edgesDF.filter (e => e.relation_label == label)
var label : String = "actively_involved_in"
println (filterEdges(label, edgesDF).collect ())
var relsTable = MorpheusRelationshipTable(
  label,
  // edgesDF.filter (e => e.relation_label == label).toDF (
  filterEdges (label, edgesDF).toDF (
    "id", "source", "target", "relation", "relation_label", "predicate_id"))


println ("Build a node table for the supported node type.")
var nodeTable = MorpheusNodeTable(Set("Gene"), nodesDF.toDF)

println ("Initialize Morpheus...")
implicit val morpheus: MorpheusSession = MorpheusSession.local()
val spark = morpheus.sparkSession

println ("Create Morpheus graph from nodes and edges.")
val graph = morpheus.readFrom(nodeTable, relsTable)

println ("Execute Cypher query and print results")
val result = graph.cypher("MATCH (n:Gene) RETURN n.name")

// Collect results into string by selecting a specific column.
// This operation may be very expensive as it materializes results locally.
//val names: Set[String] = result.records.table.df.collect().map(_.getAs[String]("name")).toSet
val names: Set[String] = result.records.table.df.take(10).map(_.getAs[String]("n_name")).toSet

println(names)
println("Cleaning up . . .")
val cmdRc = Seq("/bin/sh", "-c", "rm target/*.csv.min && rm -rf target/nodes.csv.tmp")!
println ("Done.")

