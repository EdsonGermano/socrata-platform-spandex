package com.socrata.spandex.secondary

import com.typesafe.config.{ConfigFactory, Config}

import com.socrata.spandex.common.SpandexConfig
import com.socrata.spandex.common.client.SpandexElasticSearchClient

class SpandexSecondary(config: SpandexConfig) extends SpandexSecondaryLike {
  // Use any config we are given by the secondary watcher, falling back to our locally defined config if not specified
  // The SecondaryWatcher isn't setting the context class loader, so for now we tell ConfigFactory what classloader
  // to use so we can actually find the config in our jar.
  def this(rawConfig: Config) = this(new SpandexConfig(rawConfig.withFallback(
    ConfigFactory.load(classOf[SpandexSecondary].getClassLoader).getConfig("com.socrata.spandex"))))

  val client = new SpandexElasticSearchClient(config.es)
  val index  = config.es.index
  val batchSize = config.es.dataCopyBatchSize

  init(config)

  def shutdown(): Unit = client.close()
}
