package org.renci.t2.parser

import org.apache.spark.sql.DataFrame
import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.renci.t2.util.Downloader


trait KGXFileReader {
  /**
   *  Creates morpheus element table
   */
  def convertKGXFileToDataFrame(filePath: String, session: MorpheusSession): DataFrame = {
    /**
     * Creates a dataframe from a json formatted kgx file
     */
    val content = Downloader.getFileAsString(filePath)
    val strippedJson = content.toString.stripLineEnd
    import session.sparkSession.implicits._
    val fileDF = session.sparkSession.read.option("inferSchema", "true").json(Seq(strippedJson).toDS()).toDF
    fileDF.cache()
    fileDF.checkpoint(true)
  }

  def createElementTables(filePath: String, session:MorpheusSession): Seq[MorpheusElementTable]
}
