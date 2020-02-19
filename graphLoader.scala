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
//case class Gene(id:Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, gene_node_type: String) extends Node
//case class Gene(id:String, category:String, name:String, equivalent_identifiers:String, gene_node_type: String) extends Node
//case class Disease(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, disease_node_type: String) extends Node
//case class Disease(id: String, category:String, name:String, equivalent_identifiers:String, disease_node_type: String) extends Node

//@RelationshipType("CAUSES_CONDITION")
//case class GeneToDiseaseRelationship(id:Long, source:Long, target:Long, subject:String, obj:String, relation:String, predicate_id:String, relation_label:String) extends Relationship
//case class GeneToDiseaseRelationship(id:Long, source:String, target:String, relation:String, predicate_id:String, relation_label:String) extends Relationship


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

   def readData(source: String): DataFrame = {
      spark.read.format("csv").option("header", "true").option("quote", "\"").option("escape", "\"").load(source)
   }

   def cleanData(df: DataFrame): DataFrame = {
      if (df.columns.toSeq.contains("subject")) {
         // Its an edge dataframe, so select/order edge columns
// TEST: Idea for edges is to use id column as id so:
//    1) use existing id column as id
//    2) don't add unique_id_col
//    3) can then automatically use edge subject and object as source and target columns: Just rename and change type to String
//         val tmpDf_w_id = addUniqueIdCol(df)
         val tmpDf_renamed = df.withColumnRenamed("subject","source_id")
                                .withColumnRenamed("object", "target_id")
//         val tmpDf_w_src = tmpDf_w_id.withColumn("source", lit(0L))
//         val tmpDf_w_tgt = tmpDf_w_src.withColumn("target", lit(0L))
//         val tmpDf_ord = tmpDf_renamed.select("rel_id", "source_id", "target_id", "subject", "object", "relation", "predicate_id", "relation_label")
         tmpDf_renamed.select("id", "source_id", "target_id", "relation", "predicate_id", "relation_label")
//         val tmpDf_relsrc = tmpDf_ord.withColumn("source", when(col("subject").equalTo("HGNC:12442"), 8).otherwise(col("source")))
//         tmpDf_relsrc.withColumn("target", when(col("object").equalTo("MONDO:0002022"), 23).otherwise(col("target")))
      }
      else {
         // Its a node dataframe . . .

// TEST: Idea for nodes is to use id column as id so:
//   1) don't rename id column to curie_or_id
//   2) don't add unique_id_col
//   3) don't select those two columns
//         val tmpDf = df.withColumnRenamed("id","curie_or_id")
//         var tmpDf_w_id = addUniqueIdCol(tmpDf)
//         tmpDf_w_id.select("id", "curie_or_id", "category", "name", "equivalent_identifiers")
         df.select("id", "category", "name", "equivalent_identifiers")
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
val edgesDfFromFile = csvFileDataSource.readData("target/edges.csv")
edgesDfFromFile.show(125)

println("   Cleaning Edge data  . . . " )
val edgesDf = csvFileDataSource.cleanData(edgesDfFromFile)
edgesDf.show(10000, false)

//------------------------------------------------------------------------------
// Node Data:
//------------------------------------------------------------------------------
println("   Reading node data from file and creating dataframe . . .")
val nodesDfFromFile = csvFileDataSource.readData("target/nodes.csv")

println("   Cleaning node data . . .")
val nodesDf = csvFileDataSource.cleanData(nodesDfFromFile)
nodesDf.show(125, false)

println ("show edge and node schemas.")
edgesDf.schema.printTreeString
nodesDf.schema.printTreeString

// Select gene part and create a gene-only dataframe
val geneDf:DataFrame = nodesDf.filter($"category" === "named_thing|gene")
geneDf.show(125, false)

// Select disease part and create a disease-only dataframe
val diseaseDf:DataFrame = nodesDf.filter($"category" === "named_thing|disease")
diseaseDf.show(125)


println ("Initialize Morpheus...")
implicit val morpheus: MorpheusSession = MorpheusSession.local()


val geneTable = MorpheusNodeTable(Set("Gene"), geneDf)
val diseaseTable = MorpheusNodeTable(Set("Disease"), diseaseDf)

val geneToDiseaseRelationshipTable = MorpheusRelationshipTable("relation", edgesDf.toDF())

val graph = morpheus.readFrom(geneTable, diseaseTable, geneToDiseaseRelationshipTable)

// Execute query
val finalResult = graph.cypher("MATCH (g:Gene {name: 'TYR'})--(d:Disease) RETURN d")


println("")
println("")
println("========================= DONE ============================")
println("")
println("")




object GeneDiseaseData {

   import spark.implicits._
   val genesDF: DataFrame = spark.createDataset(Seq(Gene(1, "HGNC:12442", "named_thing|gene", "TYR", "['UniProtKB:L8B082']", "gene"))).toDF("id", "curie_or_id", "category", "name", "equivalent_identifiers", "gene_node_type")
   val diseasesDF: DataFrame = spark.createDataset(Seq(Disease(2, "MONDO:0002022", "named_thing|disease", "disease_of_orbital_region", "['MEDDRA:10015903']", "disease"))).toDF("id", "curie_or_id", "category", "name", "equivalent_identifiers", "disease_node_type")
   val causesDF: DataFrame = spark.createDataset(Seq(GeneToDiseaseRelationship(1, 1, 2, "HGNC:12442", "MONDO:0002022", "RO:0003303", "RO:0002410", "causes_condition"))).toDF("id", "source", "target", "subject", "obj", "relation", "predicate_id", "relation_label")

   var geneTable = MorpheusNodeTable( Set("Gene"), genesDF)
   var diseaseTable = MorpheusNodeTable( Set("Disease"), diseasesDF)
   var causeTable = MorpheusRelationshipTable( "causes_condition", causesDF)

   implicit val morpheus: MorpheusSession = MorpheusSession.local()
   val graph = morpheus.readFrom(geneTable, diseaseTable, causeTable)
}

println("\n\nQuerying: MATCH (g:Gene {name:TYR})-[r]-(d:Disease) RETURN g.name, r.relation_label, d.name\n\n")
val result1 = GeneDiseaseData.graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN g.name, r.relation_label, d.name")
result1.show

println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN g\n\n")
val result2 = GeneDiseaseData.graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN g")
result2.show

val result3 = GeneDiseaseData.graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN d")
result3.show

val result4 = GeneDiseaseData.graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN r")
result4.show
