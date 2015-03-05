package com.socrata.spandex.secondary

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLText, SoQLType, SoQLValue}
import com.socrata.spandex.common.{SpandexBootstrap, SpandexConfig}
import wabisabi.{Client => ElasticSearchClient}

import scala.concurrent.Await
import scala.util.Try

class SpandexSecondary(conf: SpandexConfig) extends Secondary[SoQLType, SoQLValue] {
  private[this] val esc = new ElasticSearchClient(conf.esUrl)
  private[this] val index = conf.index
  private[this] val indices = List(index)
  private[this] val indexSettings = conf.indexSettings
  private[this] val mappingBase = conf.indexBaseMapping
  private[this] val mappingCol = conf.indexColumnMapping
  private[this] val bulkBatchSize = conf.bulkBatchSize
  private[this] val matchall = "{\"query\": { \"match_all\": {} } }"

  init()

  def init(): Unit = {
    SpandexBootstrap.ensureIndex(conf)
  }

  def shutdown(): Unit = { }

  def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    Await.result(esc.deleteByQuery(indices, Seq(datasetInternalName), matchall), conf.escTimeout).getResponseBody
  }

  def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] = Set.empty

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = cookie

  def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = ???

  def wantsWorkingCopies: Boolean = true

  def currentVersion(datasetInternalName: String, cookie: Cookie): Long = ???

  override def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie,
                       events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    doVersion(datasetInfo, dataVersion, cookie, events)
  }

  def doVersion(secondaryDatasetInfo: DatasetInfo, newDataVersion: Long, // scalastyle:ignore cyclomatic.complexity
                cookie: Cookie, events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    val fourbyfour = secondaryDatasetInfo.internalName

    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

    // got working copy event
    if (wccEvents.hasNext) wccEvents.next()

    if (wccEvents.hasNext) {
      val msg = s"Got ${wccEvents.size + 1} leading WorkingCopyCreated events, only support one in a version"
      throw new UnsupportedOperationException(msg)
    }

    updateMapping(fourbyfour)

    // TODO: handle version number invalid -> resync
    if (newDataVersion == -1) throw new UnsupportedOperationException(s"version $newDataVersion already assigned")

    // TODO: elasticsearch add index routing
    remainingEvents.foreach {
      case Truncated => dropCopy(fourbyfour, truncate = true)
      case ColumnCreated(secColInfo) => updateMapping(fourbyfour, Some(secColInfo.systemId.underlying.toString))
      case ColumnRemoved(secColInfo) => { /* TODO: remove column */ }
      case RowDataUpdated(ops) => ???
      case DataCopied => ??? // working copy
      case WorkingCopyPublished => ??? // working copy
      case SnapshotDropped(info) => dropCopy(fourbyfour)
      case WorkingCopyDropped => dropCopy(fourbyfour)
      case i: Any => throw new UnsupportedOperationException(s"event not supported: '$i'")
    }

    // TODO: set new version number

    cookie
  }

  override def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]],
                      cookie: Cookie, rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                      rollups: Seq[RollupInfo]): Cookie = {
    doResync(datasetInfo, copyInfo, schema, cookie, rows)
  }

  private[this] def doResync(secondaryDatasetInfo: DatasetInfo, secondaryCopyInfo: CopyInfo,
                      newSchema: ColumnIdMap[ColumnInfo[SoQLType]], cookie: Cookie,
                      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Cookie = {
    val fourbyfour = secondaryDatasetInfo.internalName
    dropCopy(fourbyfour)
    updateMapping(fourbyfour)

    val columns = newSchema.filter((_, i) => i.typ == SoQLText).keySet
    columns.foreach { i => updateMapping(fourbyfour, Some(i.underlying.toString)) }

    val sysIdCol = newSchema.values.find(_.isSystemPrimaryKey).
      getOrElse(throw new RuntimeException("missing system primary key")).systemId

    for {iter <- rows} {
      iter.grouped(bulkBatchSize).foreach { bi =>
        for (row: ColumnIdMap[SoQLValue] <- bi) {
          val docId = row(sysIdCol)
          val kvp = columns.foreach { i =>
            (i, row.getOrElse(i, ""))
          }
        }
      }
    }

    cookie
  }

  private[this] def updateMapping(fourbyfour: String, column: Option[String] = None): String = {
    val previousMapping = Await.result(esc.getMapping(indices, Seq(fourbyfour)), conf.escTimeoutFast).getResponseBody
    val cs: List[String] = Try(new JPath(JsonReader.fromString(previousMapping)).*.*.down(fourbyfour).
      down("properties").finish.collect { case JObject(fields) => fields.keys.toList }.head).getOrElse(Nil)

    val newColumns = column match {
      case Some(c) if !cs.contains(c) => c::cs
      case _ => cs
    }
    val newMapping = mappingBase.format(fourbyfour, newColumns.map(mappingCol.format(_)).mkString(","))

    Await.result(esc.putMapping(indices, fourbyfour, newMapping), conf.escTimeoutFast).getResponseBody
  }

  private[this] def dropCopy(fourbyfour: String, truncate: Boolean = false): Unit = {
    Await.result(esc.deleteByQuery(indices, Seq(fourbyfour), matchall), conf.escTimeout).getResponseBody
    if (!truncate) {
      // TODO: remove dataset mapping as well
    }
  }
}
