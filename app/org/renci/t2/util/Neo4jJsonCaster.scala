package org.renci.t2.util

import org.opencypher.morpheus.api.value.{MorpheusNode, MorpheusRelationship}
import org.opencypher.morpheus.impl.table.SparkTable
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords

import scala.collection.immutable.HashMap


object Neo4jJsonCaster {

  /**
   * Wraps a value with quotes.
   * @param value any object with toString method
   * @return string with quotes around a value.
   */
  private def wrapWithQuotes(value: Any): String = {
    "\"" + value.toString + "\""
  }

  /**
   * Parses a list to json, all inner types are converted to strings.
   * @param lst list of objects
   * @return JSON list string
   */
  private def parseList(lst : List[Any]): String = {
    var result = "["
    try {
      result += lst.map(wrapWithQuotes).reduce((A, B) => A + "," + B)
    } catch {
      case e: UnsupportedOperationException =>
    }
    result + "]"
  }

  /**
   * Converts Array containing sets of primitive objects to JSON formatted for
   * "results"
   * @param arr Array of sets of primitive types.
   * @return JSON string
   */
  private def convertArrayOfObjectToJson(arr: Iterable[Set[Any]]): String = {
    var outer = Seq[String]()
    for ( row <- arr) {
      var inner = Seq[String]()
      for (col <- row) {
        try {
          var asHashMap = col.asInstanceOf[HashMap[String, Any]]
          var toConcat = asHashMap.map { case (key, value) =>
            var newVal = wrapWithQuotes(value)
            value match {
              case list: List[Any] => newVal = parseList(list)
              case _ =>
            }
            wrapWithQuotes(key) + ":" + newVal
          }
          var concatenated = "{" + toConcat.reduce((a, v) => a + "," + v) + "}"
          inner = inner :+ concatenated
        } catch {
          case e: ClassCastException =>
            inner = inner :+ wrapWithQuotes(col.toString)
        }
      }
      // @TODO Fill metadata with type of variables (edge/Node) and internal ID
      val metaDataJson = "[" + List.fill(inner.length)("null").reduce((a,v) => a + "," + v ) + "]"
      outer = outer :+ "{\"row\": [" + inner.reduce((a, v) => a + "," + v) + "], \"metadata\": " + metaDataJson + "}"


    }
    "[" + outer.reduce((a, v) => a + ", " + v) + "]"
  }

  def convertRecordsToJson(records: RelationalCypherRecords[SparkTable.DataFrameTable]): String = {
    val cols = records.header.vars.map(_.name)
    val mappedRecords = records.rows.toArray.map(row => cols.map(col => row(col)))

    val mappedAsPrimitives = mappedRecords.map(x => x.map(_.unwrap).map{
      case node: MorpheusNode => node.properties.unwrap
      case edge: MorpheusRelationship => edge.properties.unwrap
      case other => other })
    val dataJson = "\"data\": " + convertArrayOfObjectToJson(mappedAsPrimitives)
    val colsJson:String = "\"columns\": " + parseList(cols.toList)
    val errorsJson:String = "\"errors\": {}"
    var resultsJson:String = "\"results\": [{" +
      Seq[String](colsJson, dataJson).reduce((a, v)=> a + "," + v) +
      "}]"
    "{" + Seq(resultsJson, errorsJson).reduce((a, v)=> a + "," + v) + "}"
  }


}