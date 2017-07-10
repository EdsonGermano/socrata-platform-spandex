package com.socrata.spandex.common

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

class SpandexConfig(config: Config = ConfigFactory.load().getConfig("com.socrata.spandex")) {
  val spandexPort = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val es = new ElasticSearchConfig(config.getConfig("elastic-search"))
  val suggestSize = config.getInt("suggest-size")

  val debugString = config.root.render()
}

class ElasticSearchConfig(config: Config) {
  val host = config.getString("host")
  val port = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val clusterName = config.getString("cluster-name")
  val index = config.getString("index")

  val dataCopyBatchSize = config.getInt("data-copy-batch-size")
  val dataCopyTimeout = config.getDuration("data-copy-timeout", TimeUnit.MILLISECONDS)
}
