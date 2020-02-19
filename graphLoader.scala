import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{Node, Relationship, RelationshipType}
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable, MorpheusElementTable}
import org.opencypher.okapi.api.graph.{PropertyGraph}
import scala.collection.mutable.{ListBuffer, Map}
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession}
import org.apache.spark.sql.types.{LongType, StructType, StructField}

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
// 1) DONE: Add method to index data in gene and disease (or whole) dataframes
//      --> Do it whole file at a time since now ids can be guaranteed not to change.
//
// 2) DONE: Better handle problem of relations and relation_labels that are String Arrays
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
// 4) DONE: for exec json, nodeDF.category needs to be changed from array_of_string to string
//                (see relation and relation_label changes elsewhere)
//             OR in createFilteredFrame, use list or array method to check for category type
//                  val filteredDf:DataFrame = df.filter($"category".contains(category))
//
// 5) See if there's a better way to force UDF evaluation than what's currently done in
//    triggerUDFEvaluation.Scala wants to lazily evaluate so UDFs don't 
//    actually execute until after the dataframe is split, causing the globally unique
//    id's to lose their uniqueness. From my research, so far, this is the only way to force 
//    UDF's to run, though it's ridiculously costly in realtime.
//
// 6) Figure out how to get graphs to be generally available outside classes (catalog?) so
//    they're available for queries non-programatically.
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

println("    Registering UDF's . . .")
spark.udf.register("nextNodeIdUDF", (v: Long) => v + AtomicLongIdGen.nextNodeId())
spark.udf.register("nextEdgeIdUDF", (v: Long) => v + AtomicLongIdGen.nextEdgeId())

println("    Defining DataSource trait")
trait DataSource {
   def getData(source: String, subject: String, outfile: String): Unit
   def readData(source: String): DataFrame
   def cleanEdgeData(df: DataFrame): DataFrame
   def cleanNodeData(df: DataFrame): DataFrame
}


println("   Defining csvFileDataSource object")
object csvFileDataSource extends DataSource {

   def getData(source: String, subject: String, outfile: String): Unit = { println("    Unimplemented.") }

   def readData(inputFile: String): DataFrame = {
      println(s"   READING $inputFile FROM FILE AND CREATING DATAFRAME . . .")
      spark.read.format("csv").option("header", "true").option("quote", "\"").option("escape", "\"").load(inputFile)
   }

   def writeData(df: DataFrame, outputFile: String): Unit = {
      println(s"    WRITING $outputFile TO FILE.")
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
}

object csvExecutor {

   def prepareEdgeData(): DataFrame = {
      val edgesDfFromFile = csvFileDataSource.readData("target/edges_max.csv")
      val edgesDf = csvFileDataSource.cleanEdgeData(edgesDfFromFile)
      edgesDf.show(10000, false)
      edgesDf
   }

   def prepareNodeData(): DataFrame = {
      val nodesDfFromFile = csvFileDataSource.readData("target/nodes_max.csv")
      val nodesDf = csvFileDataSource.cleanNodeData(nodesDfFromFile)
      nodesDf.show(125, false)
      nodesDf
   }

   def triggerUDFEvaluation(df: DataFrame, s: String): DataFrame = {
      var fileWriteStr = "target/" + s + ".csv"
      var fileReadStr = fileWriteStr + "/part*.csv"
      csvFileDataSource.writeData(df, fileWriteStr)
      val newDf = csvFileDataSource.readData(fileReadStr)
      println("   NEWLY READ IN df FROM FILE:")
      newDf.show(2000)
      newDf
   }

   def createFilteredFrame(df: DataFrame, category: String, nodeTypeCol: String, nodeType: String): DataFrame = {
      val filteredDf:DataFrame = df.filter($"category".contains(category))
      val filteredDf2 = filteredDf.withColumn(nodeTypeCol, lit(nodeType))
      println("   FILTERED DATAFRAME:")
      filteredDf2.show(1000)
      filteredDf2
   }

   def executeQueries(graph: PropertyGraph): Unit = {

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
   }

   def execute(): Unit = {

      val edgesDf = prepareEdgeData()
      val nodesDf = prepareNodeData()

      println ("    Show edge and node schemas . . .")
      edgesDf.schema.printTreeString
      nodesDf.schema.printTreeString

      val new_nodesDf = triggerUDFEvaluation(nodesDf, "nodes_cleaned")
      val new_edgesDf = triggerUDFEvaluation(edgesDf, "edges_cleaned")

      val geneDf2 = createFilteredFrame(new_nodesDf, "named_thing|gene", "gene_node_type", "gene")
      val diseaseDf2 = createFilteredFrame(new_nodesDf, "named_thing|disease", "disease_node_type", "disease")

      println ("   Initialize Morpheus...")
      implicit val morpheus: MorpheusSession = MorpheusSession.local()

      println("   Create Morpheus tables from dataframes . . .")
      val geneTable = MorpheusNodeTable(Set("Gene"), geneDf2)
      val diseaseTable = MorpheusNodeTable(Set("Disease"), diseaseDf2)
      val geneToDiseaseRelationshipTable = MorpheusRelationshipTable("GENE_TO_DISEASE", new_edgesDf.toDF())

      println("   Create graph from Morpheus tables . . . ")
      val graph = morpheus.readFrom(geneTable, diseaseTable, geneToDiseaseRelationshipTable)

      executeQueries(graph)

      println("    Cleaning up temp files . . .")
      val r2 = Seq("/bin/sh", "-c", "rm -rf target/*_cleaned.csv").!!

      println("\n\n========================= DONE ============================\n\n")
   }
}




println("   Defining jsonFileDataSource object")
object jsonFileDataSource extends DataSource {

   def getData(source: String, subject: String, outfile: String): Unit = { println("    Unimplemented.") }

   def readData(inputFile: String): DataFrame = {
      println(s"    READING $inputFile FROM FILE.")
      spark.read.format("json").option("multiLine", "true").option("allowSingleQuotes", "true").option("allowUnquotedFieldNames", "true").load(inputFile)
   }

   def writeData(df: DataFrame, outputFile: String): Unit = {
      println(s"    WRITING $outputFile TO FILE.")
      df.coalesce(1).write.format("csv").option("header", "true")
                                        .option("quote", "\"")
                                        .option("escape", "\"")
                                        .save(outputFile)
   }

   def cleanEdgeData(df: DataFrame): DataFrame = {
      val edgesStructDf = df.select(explode($"edges"))
      val edgesFileDf = edgesStructDf.select($"col.subject", $"col.object", $"col.relation", $"col.predicate_id", $"col.relation_label")
      val edgesFixedRelColsDf = cleanRelationColumnsData(edgesFileDf)

      val tmpDf_w_id = addUniqueIdCol(edgesFixedRelColsDf)
      tmpDf_w_id.select("id", "subject", "object", "relation", "predicate_id", "relation_label")
   }

   def cleanRelationColumnsData(df: DataFrame): DataFrame = {
      // NOTE: The code below is a short term solution which loses some data by always taking the first item in the arrays.
      // 98% of the time this is correct.  Long term, in rows with more than one relation/relation_label, we want to:
      //  a) Remove duplicate entries,
      //  b) When there are differing entries in the relation/relation_label arrays,
      //      i) Create a duplicate row for the second entries with a new unique id, and take the second items in the
      //         relation and relation_label columns
      //     ii) It may be better to do this before creating any unique id's above, so it only has to be done once.
      df.withColumn("relation", $"relation".getItem(0))
        .withColumn("relation_label", $"relation_label".getItem(0))
   }

   def cleanNodeData(df: DataFrame): DataFrame = {
      val nodesStructDf = df.select(explode($"nodes"))
      val nodesFileDf = nodesStructDf.select($"col.id", $"col.category", $"col.name", $"col.equivalent_identifiers")
      val tmpDf = nodesFileDf.withColumnRenamed("id","curie_or_id")
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
}


object jsonExecutor {

   def readJsonData(): DataFrame = {
      println("   READING JSON FILE . . .")
      val jsonDf = jsonFileDataSource.readData("target/robodb2.json")
      jsonDf
   }

   def prepareEdgeData(df: DataFrame): DataFrame = {
      println(    "CLEANING EDGES DATAFRAME . . .")
      val edgesArrayDf = df.select("edges")
      val edgesDf = jsonFileDataSource.cleanEdgeData(edgesArrayDf)
      edgesDf.show(10000, false)
      edgesDf
   }

   def prepareNodeData(df: DataFrame): DataFrame = {
      println("   CLEANING NODES DATAFRAME . . .")
      val nodesArrayDf = df.select("nodes")
      val nodesDf = jsonFileDataSource.cleanNodeData(nodesArrayDf)
      nodesDf.show(125, false)
      nodesDf
   }

   def createFilteredFrame(df: DataFrame, category: String, nodeTypeCol: String, nodeType: String): DataFrame = {
      val filteredDf:DataFrame = df.where(array_contains(df("category"), category))
      val filteredDf2 = filteredDf.withColumn(nodeTypeCol, lit(nodeType))
      println("   FILTERED DATAFRAME:")
      filteredDf2.show(1000, false)
      filteredDf2
   }

   def createIndexMap(df: DataFrame): Map[String, Long] = {
      val tempDF = df.select("curie_or_id", "id")
      tempDF.rdd.map(row => (row.getString(0) -> row.getLong(1))).collectAsMap().asInstanceOf[scala.collection.mutable.Map[String,Long]]
   }

   def triggerUDFEvaluation(df: DataFrame, s: String): DataFrame = {
      var fileWriteStr = "target/" + s + ".parquet"
      var fileReadStr = fileWriteStr + "/part*.parquet"
      df.write.parquet(fileWriteStr)
      val newDf = spark.read.parquet(fileReadStr)
      println("   NEWLY READ IN df FROM FILE:")
      newDf.show(2000, false)
      newDf
   }

   def executeQueries(graph: PropertyGraph): Unit = {

      println("\n\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN g.name, g.curie_or_id, r.relation_label, d.name, d.curie_or_id\n\n")
      var r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN g.name, g.curie_or_id, r.relation_label, d.name, d.curie_or_id")
      r.show

      //println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN g\n\n")
      //r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN g")
      //r.show

      //println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN d\n\n")
      //r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN d")
      //r.show

      println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN r\n\n")
      r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN r")
      r.show

      println("\n\nQuerying: MATCH (g:Gene)-[r]-(d:Disease) RETURN r\n\n")
      r = graph.cypher("MATCH (g:Gene)-[r]-(d:Disease) RETURN r.subject, g.name, r.relation_label, r.object, d.name")
      r.show

      println("\n\nQuerying: MATCH (g:Gene {name: 'TYR'})-[r]-(d:Disease) RETURN r\n\n")
      r = graph.cypher("MATCH (g:Gene {name: 'TYR'})-[r]-(d:Disease) RETURN r.subject, g.name, r.relation_label, r.object, d.name")
      r.show

      println("\n\nQuerying: MATCH (g:Gene {name: 'TYR'})-[r]-(d:Disease {curie_or_id: 'MONDO:0002022'}) RETURN r\n\n")
      r = graph.cypher("MATCH (g:Gene {name: 'TYR'})-[r]-(d:Disease {curie_or_id: 'MONDO:0002022'}) RETURN r.subject, g.name, r.relation_label, r.object, d.name")
      r.show
   }

   def execute(): Unit = {

      val jsonDf = readJsonData()
      val edgesDf = prepareEdgeData(jsonDf)
      val nodesDf = prepareNodeData(jsonDf)

      edgesDf.schema.printTreeString()
      nodesDf.schema.printTreeString()

      val new_edgesDf = triggerUDFEvaluation(edgesDf, "edges_cleaned")
      val new_nodesDf = triggerUDFEvaluation(nodesDf, "nodes_cleaned")

      println("    AFTER XFM, SCHEMAS:")
      new_edgesDf.schema.printTreeString()
      new_nodesDf.schema.printTreeString()

      println("    CREATING subjObjIdMap . . .")
      val subjObjIdMap = createIndexMap(new_nodesDf)

      // Create map function using subjObjIdMap and use it on all columns to set source and target
      // Then create columns from result
      // NOTE: Move to mapPartitions instead of map as data gets larger (and for better efficiency)
      println("    SETTING SOURCE AND TARGET INDICES . . .")
      def transformRow(row: Row): Row =  Row.fromSeq(row.toSeq ++ Array[Long](subjObjIdMap(row.getString(row.fieldIndex("subject"))), subjObjIdMap(row.getString(row.fieldIndex("object")))))
      def transformRows(iter: Iterator[Row]): Iterator[Row] = iter.map(transformRow)
      val newSchema = StructType(new_edgesDf.schema.fields ++ Array(StructField("source", LongType, false), StructField("target", LongType, false)))
      val indexed_edgesDf = spark.createDataFrame(new_edgesDf.rdd.mapPartitions(transformRows), newSchema)
      indexed_edgesDf.printSchema()
      indexed_edgesDf.show(2000, false)


      println("    CREATING FILTERED DATAFRAMES . . .")
      val geneDf2 = createFilteredFrame(new_nodesDf, "gene", "gene_node_type", "gene")
      val diseaseDf2 = createFilteredFrame(new_nodesDf, "disease", "disease_node_type", "disease")

      println ("   INITIALIZING MORPHEUS . . .")
      implicit val morpheus: MorpheusSession = MorpheusSession.local()

      println("   CREATING MORPHEUS TABLES . . .")
      val geneTable = MorpheusNodeTable(Set("Gene"), geneDf2)
      val diseaseTable = MorpheusNodeTable(Set("Disease"), diseaseDf2)
      val geneToDiseaseRelationshipTable = MorpheusRelationshipTable("GENE_TO_DISEASE", indexed_edgesDf.toDF())

      println("   CREATING MORPHEUS GRAPHS . . . ")
      val graph = morpheus.readFrom(geneTable, diseaseTable, geneToDiseaseRelationshipTable)

      executeQueries(graph)

      println("   Cleaning up temp files . . .")
      val r2 = Seq("/bin/sh", "-c", "rm -rf target/*_cleaned.parquet").!!

      println("\n\n========================= DONE ============================\n\n")

   }
}




// csvExecutor.execute

jsonExecutor.execute
