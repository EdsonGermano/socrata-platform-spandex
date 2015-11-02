package com.socrata.spandex.secondary

import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import com.socrata.spandex.common.{CompletionAnalyzer, SpandexBootstrap, ElasticSearchConfig, SpandexConfig}
import com.socrata.spandex.common.client.SpandexElasticSearchClient
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.Logging

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

trait SpandexSecondaryLike extends Secondary[SoQLType, SoQLValue] with Logging {
  def client: SpandexElasticSearchClient
  def index: String
  def batchSize: Int

  def init(config: SpandexConfig): Unit = {
    SpandexBootstrap.ensureIndex(config.es, client)
    CompletionAnalyzer.configure(config.analysis)
  }

  def wantsWorkingCopies: Boolean = true

  def currentVersion(datasetInternalName: String, cookie: Cookie): Long =
    throw new NotImplementedError("Not used anywhere yet")

  def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long =
    throw new NotImplementedError("Not used anywhere yet")

  def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] =
    throw new NotImplementedError("Not used anywhere yet")

  def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    client.deleteFieldValuesByDataset(datasetInternalName)
    client.deleteColumnMapsByDataset(datasetInternalName)
    client.deleteDatasetCopiesByDataset(datasetInternalName)
  }

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = {
    client.deleteFieldValuesByCopyNumber(datasetInternalName, copyNumber)
    client.deleteColumnMapsByCopyNumber(datasetInternalName, copyNumber)
    client.deleteDatasetCopy(datasetInternalName, copyNumber)
    cookie
  }

  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie, events: Iterator[Event]): Cookie = {
    val handler = new VersionEventsHandler(client, batchSize)
    handler.handle(datasetInfo.internalName, dataVersion, events)
    cookie
  }

  def resync(datasetInfo: DatasetInfo,
             copyInfo: CopyInfo,
             schema: ColumnIdMap[ColumnInfo[SoQLType]],
             cookie: Cookie,
             rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
             rollups: Seq[RollupInfo]): Cookie = {
    // Delete any existing documents related to this copy
    dropCopy(datasetInfo.internalName, copyInfo.copyNumber, cookie)
    ResyncHandler(client).go(datasetInfo, copyInfo, schema, rows, batchSize)
    cookie
  }
}
