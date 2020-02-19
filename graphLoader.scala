import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{Node, Relationship, RelationshipType}
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable, MorpheusElementTable}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import sys.process._
import scala.io.Source
import scala.language.postfixOps
import java.util.concurrent.atomic.AtomicLong

// ---------------------------------------------------------------------------------------
// TODO:
// ---------------------------------------------------------------------------------------
//
// 1) Add method to index data in gene and disease (or whole) dataframes
//      --> Probably do it whole file at a time if ids can be guaranteed not to change.
//
// 2) Better handle problem of relations and relation_labels that are String Arrays
//      a) Short term:
//         Created new relation_label and relation columns consisting 
//            of first items in arrays only. This is correct 98% of the time.
//      b) Long term:
//          i) Delete duplicates
//               df.withColumn("new_rl", array_distinct($"relation_label")).show(125,false)
//         ii) Create duplicate rows for any remaining second labels, (with new unique id 
//               and taking the second item in columns)
//        iii) Convert arrays to Strings
//               val string = args.mkString(" ")--need lambda function that skips rows 
//               without a second relation_label or relation (or if its null)
//
// 3) DONE: Figure out what to do with category list (currently separated by | chars)
//    Ans: Leave this way for CSV (modify JSON list to be the same (?))
//         For CSV, do partial string search using contains.
//
// ---------------------------------------------------------------------------------------

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
   def cleanEdgeData(df: DataFrame): DataFrame
   def cleanNodeData(df: DataFrame): DataFrame
}

println("   Defining csvFileDataSource object")
object csvFileDataSource extends DataSource {

   def getData(source: String, subject: String, outfile: String): Unit = { println("Unimplemented.") }

   def readData(inputFile: String): DataFrame = {
      println(s"   READING $inputFile FROM FILE AND CREATING DATAFRAME . . .")
      spark.read.format("csv").option("header", "true").option("quote", "\"").option("escape", "\"").load(inputFile)
   }

   def writeData(df: DataFrame, outputFile: String): Unit = {
      println(s"WRITING $outputFile TO FILE.")
      df.coalesce(1).write.format("csv").option("header", "true")
                                        .option("quote", "\"")
                                        .option("escape", "\"")
                                        .save(outputFile)
   }

   def cleanEdgeData(df: DataFrame): DataFrame = {
      println("   Cleaning Edge data  . . . " )
      val tmpDf_w_id = addUniqueIdCol(df)
      val tmpDf_w_src = tmpDf_w_id.withColumn("source", lit(0L))
      val tmpDf_w_tgt = tmpDf_w_src.withColumn("target", lit(0L))
      val tmpDf_ord = tmpDf_w_tgt.select("id", "source", "target", "subject", "object", "relation", "predicate_id", "relation_label")
      //tmpDf_w_tgt.select("id", "source", "target", "subject", "obj", "relation", "predicate_id", "relation_label")

      // TODO: This needs to be generalized and moved to an index_data method!!! (See TODO #1)
      val tmpDf_relsrc = tmpDf_ord.withColumn("source", when(col("subject").equalTo("HGNC:12442"), 0).otherwise(col("source")))
      tmpDf_relsrc.withColumn("target", when(col("object").equalTo("MONDO:0002022"), 1).otherwise(col("target")))
   }

   def cleanNodeData(df: DataFrame): DataFrame = {
      println("   Cleaning node data . . .")
      val tmpDf = df.withColumnRenamed("id","curie_or_id")
      var tmpDf_w_id = addUniqueIdCol(tmpDf)
      tmpDf_w_id.select("id", "curie_or_id", "category", "name", "equivalent_identifiers")
   }

   def addUniqueIdCol(inDf: DataFrame): DataFrame = {
      val tmpDF = inDf.withColumn("id", lit(0L))
      if (tmpDF.columns.toSeq.contains("subject")) {
         // Its an edge dataframe, so use the global edge unique id counter
         tmpDF.withColumn("id", callUDF("nextEdgeIdUDF", $"id"))
      }
      else {
         // its a node dataframe . . .
         tmpDF.withColumn("id", callUDF("nextNodeIdUDF", $"id"))
      }
   }

   //def setEdgeSourceAndTargetCols(inEdgeDf: DataFrame, inNodeDf: DataFrame): DataFrame = {
      //val tmpDf_relsrc = tmpDf_ord.withColumn("source", when(col("subject").equalTo("HGNC:12442"), 0).otherwise(col("source")))
      //tmpDf_relsrc.withColumn("target", when(col("object").equalTo("MONDO:0002022"), 1).otherwise(col("target")))

      //val tmpEdgeDf = inEdgeDf.withColumn("source", when(inEdgeDf.col("subject").equalTo(inNodeDf.col("curie_or_id"), inNodeDf.col("id")).otherwise(0)))
      //tmpEdgeDf.withColumn("target", when(tmpEdgeDf.col("obj").equalTo(inNodeDf.col("curie_or_id"), inNodeDf.col("id")).otherwise(0)))
   //}

}

object csvExecutor {

   def execute(): Unit = {
      // break this into individual methods.

      //------------------------------------------------------------------------------
      // Edge Data:
      //------------------------------------------------------------------------------
      val edgesDfFromFile = csvFileDataSource.readData("target/edges_max.csv")
      val edgesDf = csvFileDataSource.cleanEdgeData(edgesDfFromFile)
      edgesDf.show(10000, false)

      //------------------------------------------------------------------------------
      // Node Data:
      //------------------------------------------------------------------------------
      val nodesDfFromFile = csvFileDataSource.readData("target/nodes_max.csv")
      val nodesDf = csvFileDataSource.cleanNodeData(nodesDfFromFile)
      nodesDf.show(125, false)

      println ("    Show edge and node schemas . . .")
      edgesDf.schema.printTreeString
      nodesDf.schema.printTreeString


      // This code is required to actually trigger the UDF's that were called earlier that 
      // were intended to create unique id's. Scala wants to lazily evaluate so they don't 
      // get called until after the dataframe is split, causing the global uniqueness to
      // get lost.  From my research, this is the only way to force UDF's to run, though
      // it's ridiculously costly in realtime: write to disk and read back in.
      // I'm going to write a defect for this and hopefully it will change.

      // methodify this:

      csvFileDataSource.writeData(nodesDf, "target/nodes_cleaned.csv")
      val new_nodesDf = csvFileDataSource.readData("target/nodes_cleaned.csv/part*.csv")
      println("   NEWLY READ IN nodesdf FROM FILE:")
      new_nodesDf.show(2000)

      csvFileDataSource.writeData(edgesDf, "target/edges_cleaned.csv")
      val new_edgesDf = csvFileDataSource.readData("target/edges_cleaned.csv/part*.csv")
      println("   NEWLY READ IN edgesdf FROM FILE:")
      new_edgesDf.show(10000)


      // Select rows of df that are gene-specific based on category to create a gene-only dataframe
      println("   Create gene-only dataframe . . . ")
      val geneDf:DataFrame = new_nodesDf.filter($"category".contains("named_thing|gene"))
      val geneDf2 = geneDf.withColumn("gene_node_type", lit("gene"))
      println("   FILTERED geneDf:")
      geneDf2.show(1000)

      // Select rows of df that are disease-specific based on category to create a disease-only dataframe
      println("   Create disease-only dataframe . . . ")
      val diseaseDf:DataFrame = new_nodesDf.filter($"category".contains("named_thing|disease"))
      val diseaseDf2 = diseaseDf.withColumn("disease_node_type", lit("disease"))
      println("   FILTERED diseaseDf:")
      diseaseDf2.show(1000)

      println ("   Initialize Morpheus...")
      implicit val morpheus: MorpheusSession = MorpheusSession.local()


      println("   Create Morpheus tables from dataframes . . .")
      val geneTable = MorpheusNodeTable(Set("Gene"), geneDf2)
      val diseaseTable = MorpheusNodeTable(Set("Disease"), diseaseDf2)
      val geneToDiseaseRelationshipTable = MorpheusRelationshipTable("GENE_TO_DISEASE", new_edgesDf.toDF())

      println("   Create graph from Morpheus tables . . . ")
      val graph = morpheus.readFrom(geneTable, diseaseTable, geneToDiseaseRelationshipTable)

      // Execute queries
      println("\n\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN g.name, g.curie_or_id, r.relation_label, d.name, d.curie_or_id\n\n")
      var r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN g.name, g.curie_or_id, r.relation_label, d.name, d.curie_or_id")
      r.show

      println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN g\n\n")
      r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN g")
      r.show

      println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN d\n\n")
      r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN d")
      r.show

      println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN r\n\n")
      r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN r")
      r.show

      println("\n\nQuerying: MATCH (g:Gene {name: 'ENSG00000077498'})-[r {relation_label: 'pathogenic_for_condition'}]-(d:Disease) RETURN d\n\n")
      r = graph.cypher("MATCH (g:Gene {name: 'ENSG00000077498'})-[r {relation_label: '[pathogenic_for_condition]'}]-(d:Disease) RETURN d")
      r.show

      println("Cleaning up temp files . . .")
      val r = Seq("/bin/sh", "-c", "rm -rf target/*_cleaned.csv").!!

      println("")
      println("")
      println("========================= DONE ============================")
      println("")
      println("")
   }
}

csvExecutor.execute