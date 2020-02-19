import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{Node, Relationship, RelationshipType}
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable, MorpheusElementTable}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession}
////
//import org.apache.spark.rdd._
//import org.apache.spark.sql.SparkSession
//import collection.{breakOut, immutable._, Seq, List}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import sys.process._
import scala.io.Source
import scala.language.postfixOps
import java.util.concurrent.atomic.AtomicLong



// Morpheus: https://github.com/opencypher/morpheus
// Hygenic Scala Closures: http://erikerlandson.github.io/blog/2015/03/31/hygienic-closures-for-scala-function-serialization/
// Passing sequence to varargs: https://stackoverflow.com/questions/1832061/scala-pass-seq-to-var-args-functions


println ("    Defining case classes.")
case class Gene(id:Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, gene_node_type: String) extends Node
case class Disease(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, disease_node_type: String) extends Node

@RelationshipType("CAUSES_CONDITION")
case class GeneToDiseaseRelationship(id:Long, source:Long, target:Long, subject:String, obj:String, relation:String, predicate_id:String, relation_label:String) extends Relationship


println ("    Defining AtomicLongGenerator class.")
object AtomicLongIdGen {
   var nodeId: AtomicLong = new AtomicLong(0L)
   var edgeId: AtomicLong = new AtomicLong(0L)

   def nextNodeId(): Long = {
      nodeId.getAndIncrement()
   }
   def nextEdgeId(): Long = {
      edgeId.getAndIncrement()
   }
}

println("   Registering UDF's . . .")
spark.udf.register("nextNodeIdUDF", (v: Long) => v + AtomicLongIdGen.nextNodeId())
spark.udf.register("nextEdgeIdUDF", (v: Long) => v + AtomicLongIdGen.nextEdgeId())

println("Defining DataSource trait")
trait DataSource {
   def getData(source: String, subject: String, outfile: String): Unit
   def readData(source: String): DataFrame
   def cleanData(df: DataFrame): DataFrame
}

println("   Defining csvFileDataSource object")
object csvFileDataSource extends DataSource {

   def getData(source: String, subject: String, outfile: String): Unit = { println("Unimplemented.") }

   def readData(inputFile: String): DataFrame = {
      println(s"READING $inputFile FROM FILE.")
      spark.read.format("csv").option("header", "true").option("quote", "\"").option("escape", "\"").load(inputFile)
   }

   def writeData(df: DataFrame, outputFile: String): Unit = {
      println(s"WRITING $outputFile TO FILE.")
      df.coalesce(1).write.format("csv").option("header", "true")
                                        .option("quote", "\"")
                                        .option("escape", "\"")
                                        .save(outputFile)
   }

   def cleanData(df: DataFrame): DataFrame = {
      if (df.columns.toSeq.contains("subject")) {
         // Its an edge dataframe, so select/order edge columns
         val tmpDf_w_id = addUniqueIdCol(df)
         //val tmpDf_renamed = tmpDf_w_id.withColumnRenamed("subject","source")
         //                              .withColumnRenamed("object", "target")
         val tmpDf_w_src = tmpDf_w_id.withColumn("source", lit(0L))
         val tmpDf_w_tgt = tmpDf_w_src.withColumn("target", lit(0L))
         val tmpDf_ord = tmpDf_w_tgt.select("id", "source", "target", "subject", "object", "relation", "predicate_id", "relation_label")
         val tmpDf_relsrc = tmpDf_ord.withColumn("source", when(col("subject").equalTo("HGNC:12442"), 0).otherwise(col("source")))
         tmpDf_relsrc.withColumn("target", when(col("object").equalTo("MONDO:0002022"), 1).otherwise(col("target")))
      }
      else {
         // Its a node dataframe . . .
         val tmpDf = df.withColumnRenamed("id","curie_or_id")
         var tmpDf_w_id = addUniqueIdCol(tmpDf)
         tmpDf_w_id.select("id", "curie_or_id", "category", "name", "equivalent_identifiers")
      }
   }

   def addUniqueIdCol(inDF: DataFrame): DataFrame = {
      val tmpDF = inDF.withColumn("id", lit(0L))
      if (tmpDF.columns.toSeq.contains("subject")) {
         // Its an edge dataframe, so use the global edge unique id counter
         tmpDF.withColumn("id", callUDF("nextEdgeIdUDF", $"id"))
      }
      else {
         // its a node dataframe . . .
         tmpDF.withColumn("id", callUDF("nextNodeIdUDF", $"id"))
      }
   }
}


//------------------------------------------------------------------------------
// Edge Data:
//------------------------------------------------------------------------------
println("   Reading edge data from file and creating dataframe . . .")
val edgesDfFromFile = csvFileDataSource.readData("target/edges_max.csv")
edgesDfFromFile.show(125)

println("   Cleaning Edge data  . . . " )
val edgesDf = csvFileDataSource.cleanData(edgesDfFromFile)
edgesDf.show(10000, false)

//------------------------------------------------------------------------------
// Node Data:
//------------------------------------------------------------------------------
println("   Reading node data from file and creating dataframe . . .")
val nodesDfFromFile = csvFileDataSource.readData("target/nodes_max.csv")
//nodesDfFromFile.show(125)

println("   Cleaning node data . . .")
val nodesDf = csvFileDataSource.cleanData(nodesDfFromFile)
nodesDf.show(125, false)

println ("show edge and node schemas.")
edgesDf.schema.printTreeString
nodesDf.schema.printTreeString

// Test: Save nodesDf and edgesDf to disk to force the UDF's to run for unique_id.
// Theory is that they're not really running and that's why unique_ids change
// when filtering into gene and disease df's below.

csvFileDataSource.writeData(nodesDf, "target/nodes_cleaned.csv")
csvFileDataSource.writeData(edgesDf, "target/edges_cleaned.csv")

val new_nodesDf = csvFileDataSource.readData("target/nodes_cleaned.csv/part*.csv")
val new_edgesDf = csvFileDataSource.readData("target/edges_cleaned.csv/part*.csv")

println("NEWLY READ IN nodesdf FROM FILE:")
new_nodesDf.show(2000)

println("NEWLY READ IN edgesdf FROM FILE:")
new_edgesDf.show(10000)

// Select gene part and create a gene-only dataframe
val geneDf:DataFrame = new_nodesDf.filter($"category".contains("named_thing|gene"))
val geneDf2 = geneDf.withColumn("gene_node_type", lit("gene"))


println("FILTERED geneDf AFTER READING BACK IN FROM FILE:")
geneDf2.show(1000)

// Select disease part and create a disease-only dataframe
val diseaseDf:DataFrame = new_nodesDf.filter($"category".contains("named_thing|disease"))
val diseaseDf2 = diseaseDf.withColumn("disease_node_type", lit("disease"))


println("FILTERED diseaseDf AFTER READING BACK IN FROM FILE:")
diseaseDf2.show(1000)

println ("Initialize Morpheus...")
implicit val morpheus: MorpheusSession = MorpheusSession.local()

val geneTable = MorpheusNodeTable(Set("Gene"), geneDf2)
val diseaseTable = MorpheusNodeTable(Set("Disease"), diseaseDf2)

val geneToDiseaseRelationshipTable = MorpheusRelationshipTable("GENE_TO_DISEASE", new_edgesDf.toDF())
//GeneToDiseaseRelationship(id:Long, source:Long, target:Long, subject:String, obj:String, relation:String, predicate_id:String, relation_label:String)
//788,408,  83,   HGNC:12442,MONDO:0002022,['RO:0003303'],RO:0002410,['causes condition']

//val tmpDS = Seq(GeneToDiseaseRelationship(788,408,83,"HGNC:12442","MONDO:0002022","RO:0003303","RO:0002410","causes_condition")).toDS()
//val geneToDiseaseRelationshipTable = MorpheusRelationshipTable("CAUSES_CONDITION", tmpDS.toDF())

val graph = morpheus.readFrom(geneTable, diseaseTable, geneToDiseaseRelationshipTable)


// Execute queries
//val finalResult = graph.cypher("MATCH (g:Gene {name: 'TYR'})-[r]-(d:Disease) RETURN d")

println("\n\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN g.name, g.curie_or_id, r.relation_label, d.name, d.curie_or_id\n\n")
var r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN g.name, g.curie_or_id, r.relation_label, d.name, d.curie_or_id")
r.show

println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN g\n\n")
r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN g")
r.show

r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN d")
r.show

r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN r")
r.show

r = graph.cypher("MATCH (g:Gene {name: 'ENSG00000077498'})-[r {relation_label: 'pathogenic_for_condition'}]-(d:Disease) RETURN d")
r.show

println("Cleaning up temp files . . .")
val r = Seq("/bin/sh", "-c", "rm -rf target/*_cleaned.csv").!!

println("")
println("")
println("========================= DONE ============================")
println("")
println("")
