package org.renci.t2.util

object logger {
  def log(message: String, level: Int=0): Unit = {
//    @TODO change to logger4J
    println(message)
  }
  def info(message: String): Unit = {
    println(message)
  }
  def debug(message: String): Unit ={
    println(message)
  }
}
