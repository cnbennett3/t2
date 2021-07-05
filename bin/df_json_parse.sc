import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, collect_list}
import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords


/**
 * Given array of cypher var names along with thier types, returns
 * physical columns in spark that match those.
 * Eg. MATCH (a)-[b]-(c) return a, b.id, count(c)
 *  a is a complex type and every attribute of a will be represented as a_id, a_<attrb> etc...
 *  b.id will be b_id ,
 *  and count(c) will be count(c)
 *  In the data frame of the results.
 *
 *  So the idea here is to reverse map those so they can be returned with the cypher names, instead
 *  of Dataframe col names.
 */
def getColumnMappings(metaMap: List[(String, CypherType)], records: RelationalCypherRecords[SparkTable.DataFrameTable]): List[(String, CypherType, Seq[String])] = {  val physicalCols = records.table.physicalColumns
  val cypherColsToPhysicalColMap = metaMap.map( metaData => {
    val (cypherVarName, cypherType) = metaData
    val physicalCols = records.table.physicalColumns.filter(
      colName =>
        // for complex types such as nodes and edges,
        // physical col name starts with the cypher var name
        colName.startsWith(cypherVarName)
         ||
          // for simple types such as var.attr or count(x) or even count(x.id) we test for exact match
          // after replacing `.` of cypherVar with `_`
          colName.equals(cypherVarName.replace('.', '_')) )
      // Convert them to columns for use with select
    (cypherVarName, cypherType, physicalCols)
  })
  cypherColsToPhysicalColMap
}



def convertRecordsToJson(records: RelationalCypherRecords[SparkTable.DataFrameTable]): DataFrame = {
  val headers = records.header.vars.toList
  val metaMap = headers.map(x => (x.name, x.cypherType))
  var cypherVarsToPhysicalCols = getColumnMappings(metaMap, records)
  cypherVarsToPhysicalCols
  var df = records.table.df
  // if anything this should help :
  df.cache
  val aggExpressions = cypherVarsToPhysicalCols.map(
    x =>{
      val (cypherName, cypherType, physicalCols) = x
      collect_list(struct(
        physicalCols.map(x => col(x)//.alias(x.stripPrefix(cypherName))
        ): _*)
      ).alias(cypherName)
    }
  )
  // collect properties into cypher variables
  df.agg(aggExpressions.head, aggExpressions.tail :_*)
  // merge each aggregated list into `data` column
//  val cypherVarNames = headers.map(_.name).map(col)
//  df = df.withColumn("data",concat(cypherVarNames:_*))
}
