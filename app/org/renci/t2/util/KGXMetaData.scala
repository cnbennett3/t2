package org.renci.t2.util


import cats.syntax.either._
import io.circe._
import io.circe.generic.auto._
import io.circe.yaml


case class Version(version: String, nodeFiles: List[String], edgeFiles: List[String])
case class Versions(versions: List[Version])

/***
 *
 * @param serverRootUrl
 * @param metaDataFileName
 */
class KGXMetaData(var serverRootUrl: String, var metaDataFileName: String = "meta-data.yaml") {



  /**
   * Gets metadata about nodes and edges available.
   * @param metadataURL
   * @return Versions
   */
  private def parseFilesMetaData(metadataURL: String): Versions = {
    logger.info("Getting file" + metadataURL)
    val config: String =
      """
        |versions:
        |- version: v0.1
        |  nodeFiles:
        |   - cord19-phenotypes-node-v0.1.json
        |  edgeFiles:
        |   - cord19-phenotypes-edge-v0.1.json
        |""".stripMargin

    //Downloader.getFileAsString(this.serverRootUrl + "/" + metadataURL)
    logger.debug("Config: ")
    logger.debug(config)
    val json = yaml.parser.parse(config)
    val versions: Versions = json
      .leftMap(err => err: Error)
      .flatMap(_.as[Versions])
      .valueOr(throw _)
    versions
  }

  def getVersionData(versionStr: String): Version = {
    val versions: Versions = this.parseFilesMetaData(this.metaDataFileName)
    // Will be the first match of the version number
    versions.versions.find(_.version == versionStr).toSeq(0)
  }

}

