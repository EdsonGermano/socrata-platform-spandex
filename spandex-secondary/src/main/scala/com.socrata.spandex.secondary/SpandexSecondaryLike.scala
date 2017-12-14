package com.socrata.spandex.secondary

import com.rojoma.simplearm.Managed

import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.spandex.common.{SpandexConfig, Timings}
import com.socrata.spandex.common.SpandexConfig
import com.socrata.spandex.common.client.SpandexElasticSearchClient

trait SpandexSecondaryLike extends Secondary[SoQLType, SoQLValue] with SecondaryEventLogger {
  def client: SpandexElasticSearchClient
  def index: String
  def batchSize: Int

  def init(config: SpandexConfig): Unit = {
    logger.info("Configuration:\n" + config.debugString)
    SpandexElasticSearchClient.ensureIndex(config.es.index, client)
  }

  def time[A](f: => A): (Long, A) = {
    val now = Timings.now
    val res = f
    val elapsed = Timings.elapsedInMillis(now)
    (elapsed, res)
  }

  def currentVersion(datasetInternalName: String, cookie: Cookie): Long =
    throw new NotImplementedError("Not used anywhere yet") // scalastyle:ignore multiple.string.literals

  def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long =
    throw new NotImplementedError("Not used anywhere yet") // scalastyle:ignore multiple.string.literals

  def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    client.deleteColumnValuesByDataset(datasetInternalName, refresh = false)
    client.deleteColumnMapsByDataset(datasetInternalName, refresh = false)
    client.deleteDatasetCopiesByDataset(datasetInternalName, refresh = false)
    logRefreshRequest()
    client.refresh()
  }

  def dropCopy(datasetInfo: DatasetInfo, copyInfo: CopyInfo, cookie: Cookie, isLatestCopy: Boolean): Cookie = {
    doDropCopy(datasetInfo.internalName, copyInfo.copyNumber)
    cookie
  }

  private[this] def doDropCopy(datasetInternalName: String, copyNumber: Long): Unit = {
    client.deleteColumnValuesByCopyNumber(datasetInternalName, copyNumber, refresh = false)
    client.deleteColumnMapsByCopyNumber(datasetInternalName, copyNumber, refresh = false)
    client.deleteDatasetCopy(datasetInternalName, copyNumber, refresh = false)
    logRefreshRequest()
    client.refresh()
  }

  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie, events: Iterator[Event]): Cookie = {
    val handler = new VersionEventsHandler(client, batchSize)
    handler.handle(datasetInfo.internalName, dataVersion, events)
    cookie
  }

  def resync(
      datasetInfo: DatasetInfo,
      copyInfo: CopyInfo,
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      cookie: Cookie,
      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
      rollups: Seq[RollupInfo],
      isLatestLivingCopy: Boolean)
  : Cookie = {
    // Delete any existing documents related to this copy
    doDropCopy(datasetInfo.internalName, copyInfo.copyNumber)

    val (elapsedtime, _) = time {
      ResyncHandler(client).go(datasetInfo, copyInfo, schema, rows, batchSize)
    }

    logResyncCompleted(datasetInfo.internalName, copyInfo.copyNumber, elapsedtime)
    cookie
  }
}
