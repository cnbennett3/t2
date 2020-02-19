import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{Node, Relationship, RelationshipType}

import org.apache.spark.sql.{DataFrame, Row, Dataset}
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable, MorpheusElementTable}
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import scala.collection.mutable.ListBuffer
import spark.sqlContext.implicits._
////
import org.apache.spark.rdd._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import collection.{breakOut, immutable}
import sys.process._
import scala.io.Source
import scala.language.postfixOps




case class Gene(id:Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, gene_edge_type: String) extends Node
case class Disease(id: Long, curie_or_id:String, category:String, name:String, equivalent_identifiers:String, disease_edge_type: String) extends Node

@RelationshipType("CAUSES_CONDITION")
case class GeneToDiseaseRelationship(id:Long, source:Long, target:Long, subject:String, obj:String, relation:String, predicate_id:String, relation_label:String) extends Relationship

object GeneDiseaseData {

   import spark.implicits._
   val genesDF: DataFrame = spark.createDataset(Seq(Gene(1, "HGNC:12442", "named_thing|gene", "TYR", "['UniProtKB:L8B082']", "gene"))).toDF("id", "curie_or_id", "category", "name", "equivalent_identifiers", "gene_edge_type")
   val diseasesDF: DataFrame = spark.createDataset(Seq(Disease(2, "MONDO:0002022", "named_thing|disease", "disease_of_orbital_region", "['MEDDRA:10015903']", "disease"))).toDF("id", "curie_or_id", "category", "name", "equivalent_identifiers", "disease_edge_type")
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
