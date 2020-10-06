package org.renci.t2.util

import com.typesafe.config.ConfigFactory

/**
 * Wrapper for configuration loader
 */

object Config {

  private val config =ConfigFactory.load()

  def getConfig(configName: String): String = {
      this.config.getString(configName)
  }


}