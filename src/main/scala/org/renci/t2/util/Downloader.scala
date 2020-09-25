package org.renci.t2.util

import scala.io.BufferedSource

object Downloader {
  /**
   * Retuns a string of the file
   * @param fileName: Name of the file to get
   */
  def getFileAsString(fileUrl: String): String = {
    val bufferedSource: BufferedSource = scala.io.Source.fromURL(fileUrl, "utf-8")
    val content: String = bufferedSource.mkString
    bufferedSource.close()
    content
  }
}
