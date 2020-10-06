package org.renci.t2.app

import org.renci.t2.core.Core


object App {
  def main(args: Array[String]): Unit = {
    val core:Core = new Core()
    val graph = core.makeGraph("v0.1")
    core.runCypherAndShow(cypherQuery = "MATCH (c) return count(c)", graph = graph)

  }

}
