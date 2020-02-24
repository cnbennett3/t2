import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{Node, Relationship, RelationshipType}
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable, MorpheusElementTable}
import org.opencypher.okapi.api.graph.{PropertyGraph}
import scala.collection.mutable.Map
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession}
import org.apache.spark.sql.types.{LongType, StructType, StructField}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import sys.process._
import scala.io.Source
import scala.language.postfixOps
import java.util.concurrent.atomic.AtomicLong

println ("    Defining case classes.")
case class AnatomicalEntity(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, anatomical_entity_node_type: String) extends Node
case class BiologicalProcess(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, biological_process_node_type: String) extends Node
case class Cell(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, cell_node_type: String) extends Node
case class CellularComponent(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, cellular_component_node_type: String) extends Node
case class ChemicalSubstance(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, chem_subst_node_type: String) extends Node
case class Disease(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, disease_node_type: String) extends Node
case class DiseaseOrPhenotypicFeature(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, disease_or_phenotypic_feature_node_type: String) extends Node
case class Gene(id:Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, gene_node_type: String) extends Node
case class GeneFamily(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, gene_family_node_type: String) extends Node
case class GeneOrGeneProduct(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, gene_or_gene_product_node_type: String) extends Node
case class GeneProduct(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, gene_product_node_type: String) extends Node
case class GeneticCondition(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, genetic_condition_node_type: String) extends Node
case class MolecularActivity(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, moleculular_activity_node_type: String) extends Node
case class NamedThing(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, named_thing_node_type: String) extends Node
case class Pathway(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, pathway_node_type: String) extends Node
case class PhenotypicFeature(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, phenotypic_feature_node_type: String) extends Node

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
      //val edgesFixedRelColsDf = cleanRelationColumnsData(df)
      val tmpDf_w_id = addUniqueIdCol(df)
      val tmpDf_w_src = tmpDf_w_id.withColumn("source", lit(0L))
      val tmpDf_w_tgt = tmpDf_w_src.withColumn("target", lit(0L))
      val tmpDf_ord = tmpDf_w_tgt.select("id", "source", "target", "subject", "object", "relation", "predicate_id", "relation_label")
      tmpDf_ord
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

      val edgesId = addUniqueIdCol(edgesFixedRelColsDf)
      edgesId.select("id", "subject", "object", "relation", "predicate_id", "relation_label")
   }

   def cleanRelationColumnsData(df: DataFrame): DataFrame = {
      // NOTE: The code below is a short term solution which loses some data by always taking the first item in the arrays.
      // 98% of the time this is correct.  Long term, in rows with more than one relation/relation_label, we want to:
      //  a) Remove duplicate entries,
      //  b) When there are differing entries in the relation/relation_label arrays,
      //      i) Create a duplicate row for the second entries with a new unique id, and take the second items in the
      //         relation and relation_label columns
      //     ii) It may be better to do this before creating any unique id's above, so it only has to be done once.
      val relDf = df.withColumn("relation", $"relation".getItem(0))
                    .withColumn("relation_label", $"relation_label".getItem(0))
      relDf
   }

   def cleanNodeData(df: DataFrame): DataFrame = {
      val nodesStructDf = df.select(explode($"nodes"))
      val nodesFileDf = nodesStructDf.select($"col.id", $"col.category", $"col.name", $"col.equivalent_identifiers")
      val nodesRenColDf = nodesFileDf.withColumnRenamed("id","curie_or_id")
      var nodesId = addUniqueIdCol(nodesRenColDf)
      nodesId.select("id", "curie_or_id", "category", "name", "equivalent_identifiers")
   }

   def addUniqueIdCol(df: DataFrame): DataFrame = {
      val idDf = df.withColumn("id", lit(0L))
      if (idDf.columns.toSeq.contains("subject")) {
         // Its an edge dataframe, so use the global edge unique id counter
         idDf.withColumn("id", callUDF("nextEdgeIdUDF", $"id"))
      }
      else {
         // its a node dataframe . . .
         idDf.withColumn("id", callUDF("nextNodeIdUDF", $"id"))
      }
   }
}


object jsonExecutor {

   // Cleaning up files here so we can use the graph handle in the REPL 
   // later. If we clean up the parquet files at the end of a run, attempts
   // to run queries in the REPL manually after the program exits will fail.
   // Most likely because the DF's are now larger than memory.
   println("\n\n   Cleaning up temp files . . .")
   val r2 = Seq("/bin/sh", "-c", "rm -rf target/*_cleaned.parquet").!!

   def readJsonData(s: String): DataFrame = {
      println("   READING JSON FILE . . .")
      val jsonDf = jsonFileDataSource.readData(s)
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
      val idsDf = df.select("curie_or_id", "id")
      idsDf.rdd.map(row => (row.getString(0) -> row.getLong(1))).collectAsMap().asInstanceOf[scala.collection.mutable.Map[String,Long]]
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

      println("\n\n==================== 2 Node Queries ============================\n\n")

      println("\n\n\nQuery 1: MATCH (g:gene)-[r]-(d:disease) RETURN g.name, g.curie_or_id, r.relation_label, d.name, d.curie_or_id\n\n")
      var r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN g.name, g.curie_or_id, r.relation_label, d.name, d.curie_or_id limit 10")
      r.show

      println("\n\nQuery 2: (5) MATCH (g:gene)-[r]-(d:disease) RETURN g\n\n")
      spark.time(r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN g limit 5"))
      r.show

      println("\n\nQuery 2: (100) MATCH (g:gene)-[r]-(d:disease) RETURN g\n\n")
      spark.time(r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN g limit 100"))
      r.show

      println("\n\nQuery 2: (1000) MATCH (g:gene)-[r]-(d:disease) RETURN g\n\n")
      spark.time(r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN g limit 1000"))
      r.show

      println("\n\nQuery 2: (10000) MATCH (g:gene)-[r]-(d:disease) RETURN g\n\n")
      spark.time(r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN g limit 10000"))
      r.show

      println("\n\nQuery 2: (100000) MATCH (g:gene)-[r]-(d:disease) RETURN g\n\n")
      spark.time(r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN g limit 100000"))
      r.show

      println("\n\nQuery 3:: MATCH (g:gene)-[r]-(d:disease) RETURN d\n\n")
      r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN d limit 10")
      r.show

      println("\n\nQuery 2: MATCH (g:gene)-[r]-(d:disease) RETURN r\n\n")
      r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN r limit 10")
      r.show

      println("\n\nQuery 3: MATCH (g:gene)-[r]-(d:disease) RETURN r.subject, g.name, r.relation_label, r.object, d.name\n\n")
      r = graph.cypher("MATCH (g:gene)-[r]-(d:disease) RETURN r.subject, g.name, r.relation_label, r.object, d.name limit 10")
      r.show

      // Name here and below changed from TYR to ENSG00000077498
      println("\n\nQuery 4: MATCH (g:gene {name: 'ENSG00000077498'})-[r]-(d:disease) RETURN r.subject, g.name, r.relation_label, r.object, d.name\n\n")
      r = graph.cypher("MATCH (g:gene {name: 'ENSG00000077498'})-[r]-(d:disease) RETURN r.subject, g.name, r.relation_label, r.object, d.name limit 10")
      r.show

      println("\n\nQuery 5: MATCH (g:gene {name: 'ENSG00000077498'})-[r]-(d:disease {curie_or_id: 'MONDO:0002022'}) RETURN r.subject, g.name, r.relation_label, r.object, d.name\n\n")
      r = graph.cypher("MATCH (g:gene {name: 'ENSG00000077498'})-[r]-(d:disease {curie_or_id: 'MONDO:0002022'}) RETURN r.subject, g.name, r.relation_label, r.object, d.name limit 10")
      r.show


      println("\n\n==================== 3 Node Query Tests ============================\n\n")

      println("\n\nQuery 6:  MATCH (c:chemical_substance) RETURN * limit 100\n\n")
      r = graph.cypher("MATCH (c:chemical_substance) RETURN * limit 100")
      r.show

      println("\n\nQuery 7: MATCH (c:chemical_substance {name: 'bisphenol A'}) RETURN *\n\n")
      r = graph.cypher("MATCH (c:chemical_substance {name: 'bisphenol A'}) RETURN *")
      r.show

      println("\n\nQuery 8: MATCH (c:chemical_substance {curie_or_id: 'CHEBI:33216'})--(g:gene) RETURN *\n\n")
      r = graph.cypher("MATCH (c:chemical_substance {curie_or_id: 'CHEBI:33216'})--(g:gene) RETURN *")
      r.show

      println("\n\nQuery 9: MATCH (c:chemical_substance {name: 'bisphenol A'})<--(g:gene) RETURN *\n\n")
      r = graph.cypher("MATCH (c:chemical_substance {name: 'bisphenol A'})<--(g:gene) RETURN *")
      r.show

      println("\n\nQuery 10:  MATCH (g:gene)-->(c:chemical_substance {name: 'bisphenol A'}) RETURN g.curie_or_id, c.curie_or_id\n\n")
      r = graph.cypher("MATCH (g:gene)-->(c:chemical_substance {name: 'bisphenol A'}) RETURN g.curie_or_id, c.curie_or_id")
      r.show

      println("\n\nQuery 11: MATCH (c:chemical_substance {name: 'bisphenol A'})<--(g:gene {curie_or_id: 'HGNC:11180'})-->(d:disease) RETURN *\n\n")
      r = graph.cypher("MATCH (c:chemical_substance {name: 'bisphenol A'})<--(g:gene {curie_or_id: 'HGNC:11180'})-->(d:disease) RETURN *")
      r.show

      println("\n\nQuery 12:  MATCH (c:chemical_substance {curie_or_id: 'CHEBI:33216'})<--(g:gene {id: 'HGNC:11180'})-->(d:disease) RETURN *\n\n")
      r = graph.cypher("MATCH (c:chemical_substance {curie_or_id: 'CHEBI:33216'})<--(g:gene {curie_or_id: 'HGNC:11180'})-->(d:disease) RETURN *")
      r.show

      println("\n\nQuery 13: MATCH (c:chemical_substance {name: 'bisphenol A'})<--(g:gene {curie_or_id: 'HGNC:11180'})-->(d:disease {curie_or_id: 'MONDO:0012970'}) RETURN c.name, c.curie_or_id, g.name, g.curie_or_id, d.name, d.curie_or_id, d.equivalent_identifiers\n\n")
      r = graph.cypher("MATCH (c:chemical_substance {name: 'bisphenol A'})<--(g:gene {curie_or_id: 'HGNC:11180'})-->(d:disease {curie_or_id: 'MONDO:0012970'}) RETURN c.name, c.curie_or_id, g.name, g.curie_or_id, d.name, d.curie_or_id, d.equivalent_identifiers")
      r.show      

   }

   def executeNewQueries(graph: PropertyGraph): Unit = {

      println("\n\n==================== Query New Node Types ============================\n\n")

      // anatomical_entity
      println("\n\nQuery 5.3: MATCH (a:anatomical_entity) RETURN a\n\n")
      var r = graph.cypher("MATCH (a:anatomical_entity) RETURN a")
      r.show

      // biological_process
      println("\n\nQuery 5.4: MATCH (b:biological_process) RETURN b\n\n")
      r = graph.cypher("MATCH (b:biological_process) RETURN b")
      r.show

      // biological_process_or_activity
      println("\n\nQuery 5.5: MATCH (g:gene {curie_or_id: 'HGNC:77'})-[r]-(b:biological_process_or_activity) RETURN g.curie_or_id, b.curie_or_id\n\n")
      r = graph.cypher("MATCH (g:gene {curie_or_id: 'HGNC:77'})-[r]-(b:biological_process_or_activity) RETURN g.curie_or_id, b.curie_or_id")
      r.show

      // cell
      println("\n\nQuery 5.6: MATCH (c:cell)--(g:gene) RETURN c.curie_or_id, g.curie_or_id\n\n")
      r = graph.cypher("MATCH (c:cell)--(g:gene) RETURN c.curie_or_id, g.curie_or_id")
      r.show

      // cellular_component
      println("\n\nQuery 5.7: MATCH (c:cellular_component {curie_or_id: 'GO:0005615'})--(g:gene) RETURN c.curie_or_id, g.curie_or_id\n\n")
      r = graph.cypher("MATCH (c:cellular_component {curie_or_id: 'GO:0005615'})--(g:gene) RETURN c.curie_or_id, g.curie_or_id")
      r.show

      // disease_or_phenotypic_feature
      println("\n\nQuery 5.2: MATCH (d:disease_or_phenotypic_feature) RETURN d\n\n")
      r = graph.cypher("MATCH (d: disease_or_phenotypic_feature) RETURN d")
      r.show

      // gene_family
      println("\n\nQuery 5.9: MATCH (g:gene_family {curie_or_id: 'HGNC.FAMILY:40'})--(h:gene) RETURN g.curie_or_id, h.curie_or_id\n\n")
      r = graph.cypher("MATCH (g:gene_family {curie_or_id: 'HGNC.FAMILY:40'})--(h:gene) RETURN g.curie_or_id, h.curie_or_id")
      r.show

      // gene_or_gene_product
      println("\n\nQuery 5.9: MATCH (g:gene_or_gene_product {curie_or_id: 'CHEBI:681569'})--(h:gene) RETURN g.curie_or_id, h.curie_or_id\n\n")
      r = graph.cypher("MATCH (g:gene_or_gene_product {curie_or_id: 'CHEBI:681569'})--(h:gene) RETURN g.curie_or_id, h.curie_or_id")
      r.show

      // gene_product
      println("\n\nQuery 5.10: MATCH (g:gene_product {curie_or_id: 'CHEBI:681569'})--(h:gene) RETURN g.curie_or_id, h.curie_or_id\n\n")
      r = graph.cypher("MATCH (g:gene_product {curie_or_id: 'CHEBI:681569'})--(h:gene) RETURN g.curie_or_id, h.curie_or_id")
      r.show

      // geneticCondition
      println("\n\nQuery 5.11: MATCH (g:gene)-[r]-(h:genetic_condition {curie_or_id: 'MONDO:0015547'}) RETURN g.curie_or_id, r.relation_label, h.name\n\n")
      r = graph.cypher("MATCH (g:gene)-[r]-(h:genetic_condition {curie_or_id: 'MONDO:0015547'}) RETURN g.curie_or_id, r.relation_label, h.name")
      r.show

      // molecularActivity
      println("\n\nQuery 5.12: MATCH (g:gene)-[r]-(h:molecular_activity {curie_or_id: 'GO:0016757'}) RETURN h.curie_or_id, r.relation_label, g.curie_or_id\n\n")
      r = graph.cypher("MATCH (g:gene)-[r]-(h:molecular_activity {curie_or_id: 'GO:0016757'}) RETURN h.curie_or_id, r.relation_label, g.curie_or_id")
      r.show

      // named_thing
      println("\n\nQuery 5.13: MATCH (n:named_thing)-->(c:chemical_substance) RETURN n.curie_or_id, c.curie_or_id LIMIT 5\n\n")
      r = graph.cypher("MATCH (n:named_thing)-->(c:chemical_substance) RETURN n.curie_or_id, c.curie_or_id LIMIT 5")
      r.show

      // phenotypic_feature
      println("\n\nQuery 5.14: MATCH (p:phenotypic_feature) RETURN p\n\n")
      r = graph.cypher("MATCH (p: phenotypic_feature) RETURN p.curie_or_id, p.name, p.category LIMIT 50")
      r.show

      // pathway
      println("\n\nQuery 5.15: MATCH (g:gene)-[r]-(p:pathway {curie_or_id: 'REACT:R-HSA-114608'}) RETURN g.curie_or_id , r.relation_label, p.name\n\n")
      r = graph.cypher("MATCH (g:gene)-[r]-(p:pathway {curie_or_id: 'REACT:R-HSA-114608'}) RETURN g.curie_or_id , r.relation_label, p.name")
      r.show
   }


   def execute(): PropertyGraph = {

      //val jsonDataFile: String = "target/robodb2.json"
      val jsonDataFile: String = "target/robodb_gene_9999.json"
      val jsonDf = readJsonData(jsonDataFile)
      val edgesDf = prepareEdgeData(jsonDf)
      val nodesDf = prepareNodeData(jsonDf)

      edgesDf.schema.printTreeString()
      nodesDf.schema.printTreeString()

      val newEdgesDf = triggerUDFEvaluation(edgesDf, "edges_cleaned")
      val newNodesDf = triggerUDFEvaluation(nodesDf, "nodes_cleaned")

      println("    AFTER XFM, SCHEMAS:")
      newEdgesDf.schema.printTreeString()
      newNodesDf.schema.printTreeString()

      println("    CREATING subjObjIdMap . . .")
      val subjObjIdMap = createIndexMap(newNodesDf)

      // Create map function using subjObjIdMap and use it on all columns to set source and target
      // Then create columns from result
      // NOTE: Move to mapPartitions instead of map as data gets larger (and for better efficiency)
      println("    SETTING SOURCE AND TARGET INDICES . . .")
      def transformRow(row: Row): Row =  Row.fromSeq(row.toSeq ++ Array[Long](subjObjIdMap(row.getString(row.fieldIndex("subject"))), subjObjIdMap(row.getString(row.fieldIndex("object")))))
      def transformRows(iter: Iterator[Row]): Iterator[Row] = iter.map(transformRow)
      val newSchema = StructType(newEdgesDf.schema.fields ++ Array(StructField("source", LongType, false), StructField("target", LongType, false)))
      val indexedEdgesDf = spark.createDataFrame(newEdgesDf.rdd.mapPartitions(transformRows), newSchema)
      indexedEdgesDf.printSchema()
      indexedEdgesDf.show(2000, false)

      println("    CREATING FILTERED DATAFRAMES . . .")
      val anaEntityDf2 = createFilteredFrame(newNodesDf, "anatomical_entity", "anatomical_entity_node_type", "anatomical_entity")
      val bioProcDf2 = createFilteredFrame(newNodesDf, "biological_process", "biological_process_node_type", "biological_process")
      val bioProcOrActDf2 = createFilteredFrame(newNodesDf, "biological_process_or_activity", "biological_process_or_activity_node_type", "biological_process_or_activity")
      val cellDf2 = createFilteredFrame(newNodesDf, "cell", "cell_node_type", "cell")
      val cellCompDf2 = createFilteredFrame(newNodesDf, "cellular_component", "cellular_component_node_type", "cellular_component")
      val chemSubstDf2 = createFilteredFrame(newNodesDf, "chemical_substance", "chemical_subst_node_type", "chemical_substance")
      val diseaseDf2 = createFilteredFrame(newNodesDf, "disease", "disease_node_type", "disease")
      val disPhenFeatDf2 = createFilteredFrame(newNodesDf, "disease_or_phenotypic_feature", "dis_or_phenotypic_feature_node_type", "disease_or_phenotypic_feature")
      val geneDf2 = createFilteredFrame(newNodesDf, "gene", "gene_node_type", "gene")
      val geneFamDf2 = createFilteredFrame(newNodesDf, "gene_family", "gene_familty_node_type", "gene_family")
      val geneOrGeneProdDf2 = createFilteredFrame(newNodesDf, "gene_or_gene_product", "gene_or_gene_product_node_type", "gene_or_gene_product")
      val geneProdDf2 = createFilteredFrame(newNodesDf, "gene_product", "gene_product_node_type", "gene_product")
      val geneticCondDf2 = createFilteredFrame(newNodesDf, "genetic_condition", "genetic_condition_node_type", "genetic_condition")
      val molecularActDf2 = createFilteredFrame(newNodesDf, "molecular_activity", "molecular_activity_node_type", "molecular_activity")
      val namedThingDf2 = createFilteredFrame(newNodesDf, "named_thing", "named_thing_node_type", "named_thing")
      val pathwayDf2 = createFilteredFrame(newNodesDf, "pathway", "pathway_node_type", "pathway")
      val phenFeatDf2 = createFilteredFrame(newNodesDf, "phenotypic_feature", "phenotypic_feature_node_type", "phenotypic_feature")


      println ("   INITIALIZING MORPHEUS . . .")
      implicit val morpheus: MorpheusSession = MorpheusSession.local()

      println("   CREATING MORPHEUS TABLES . . .")
      // The names in quotes below must match the variable type name in your Cypher queries!
      val anaEntityTable = MorpheusNodeTable(Set("anatomical_entity"), anaEntityDf2)
      val bioProcTable = MorpheusNodeTable(Set("biological_process"), bioProcDf2)
      val bioProcOrActTable = MorpheusNodeTable(Set("biological_process_or_activity"), bioProcOrActDf2)
      val cellTable = MorpheusNodeTable(Set("cell"), cellDf2)
      val cellCompTable = MorpheusNodeTable(Set("cellular_component"), cellCompDf2)
      val chemSubstTable = MorpheusNodeTable(Set("chemical_substance"), chemSubstDf2)
      val diseaseTable = MorpheusNodeTable(Set("disease"), diseaseDf2)
      val disPhenFeatTable = MorpheusNodeTable(Set("disease_or_phenotypic_feature"), disPhenFeatDf2)
      val geneTable = MorpheusNodeTable(Set("gene"), geneDf2)
      val geneFamTable = MorpheusNodeTable(Set("gene_family"), geneFamDf2)
      val geneOrGeneProdTable = MorpheusNodeTable(Set("gene_or_gene_product"), geneOrGeneProdDf2)
      val geneProdTable = MorpheusNodeTable(Set("gene_product"), geneProdDf2)
      val geneticCondTable = MorpheusNodeTable(Set("genetic_condition"), geneticCondDf2)
      val molecularActTable = MorpheusNodeTable(Set("molecular_activity"), molecularActDf2)
      val namedThingTable = MorpheusNodeTable(Set("named_thing"), namedThingDf2)
      val pathwayTable = MorpheusNodeTable(Set("pathway"), pathwayDf2)
      val phenFeatTable = MorpheusNodeTable(Set("phenotypic_feature"), phenFeatDf2)

      val geneToDiseaseRelationshipTable = MorpheusRelationshipTable("GENE_TO_DISEASE", indexedEdgesDf.toDF())

      println("   CREATING MORPHEUS GRAPH . . . ")
      val graph = morpheus.readFrom(anaEntityTable, bioProcTable, bioProcOrActTable, cellTable, cellCompTable, chemSubstTable, diseaseTable, disPhenFeatTable, geneTable, 
                  geneFamTable, geneOrGeneProdTable, geneProdTable, geneticCondTable, molecularActTable, namedThingTable, pathwayTable, phenFeatTable, geneToDiseaseRelationshipTable)

      executeQueries(graph)
      executeNewQueries(graph)

      println("\n\n========================= DONE ============================\n\n")

      return graph
   }
}


// csvExecutor.execute

val rgraph = jsonExecutor.execute
