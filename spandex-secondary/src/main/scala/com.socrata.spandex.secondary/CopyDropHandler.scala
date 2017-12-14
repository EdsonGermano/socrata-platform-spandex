package com.socrata.spandex.secondary

import com.socrata.datacoordinator.id.CopyId
import com.socrata.datacoordinator.secondary.{CopyInfo, LifecycleStage}
import com.socrata.spandex.common.client._
import org.joda.time.DateTime

case class CopyDropHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  private[this] def checkStage(expected: LifecycleStage, actual: LifecycleStage): Unit =
    if (actual != expected) {
      throw InvalidStateBeforeEvent(
        s"Copy is in unexpected stage: $actual. Expected: $expected")
    }

  def dropCopy(datasetName: String, info: CopyInfo, expectedStage: LifecycleStage): Unit = {
    checkStage(expectedStage, info.lifecycleStage)
    logCopyDropped(datasetName, info.lifecycleStage, info.copyNumber)
    client.deleteDatasetCopy(datasetName, info.copyNumber, refresh = false)
    client.deleteColumnValuesByCopyNumber(datasetName, info.copyNumber, refresh = false)
    client.deleteColumnMapsByCopyNumber(datasetName, info.copyNumber, refresh = false)
    logRefreshRequest()
    client.refresh()
  }

  def dropSnapshot(datasetName: String, info: CopyInfo): Unit = {
    checkStage(LifecycleStage.Snapshotted, info.lifecycleStage)
    logSnapshotDropped(datasetName, info.copyNumber)
    client.deleteDatasetCopy(datasetName, info.copyNumber, refresh = false)
    client.deleteColumnValuesByCopyNumber(datasetName, info.copyNumber, refresh = false)
    client.deleteColumnMapsByCopyNumber(datasetName, info.copyNumber, refresh = false)
    logRefreshRequest()
    client.refresh()
  }

  def dropUnpublishedCopies(datasetName: String): Unit = {
    List(Snapshotted, Unpublished, Discarded).foreach { expectedStage =>
      client.datasetCopiesByStage(datasetName, expectedStage).foreach {
        case DatasetCopy(_, copyNumber, version, stage) =>
          val expectedLifecycleStage = expectedStage match {
            case Unpublished => LifecycleStage.Unpublished
            case Snapshotted => LifecycleStage.Snapshotted
            case Discarded => LifecycleStage.Discarded
            case _ => throw new UnexpectedCopyStage(expectedStage.name)
          }

          // NOTE: Spandex doesn't know anything about copy ID or last modified timestamp,
          // so we use stubs for those parameters when constructing this CopyInfo
          val copyInfo = CopyInfo(new CopyId(-1L), copyNumber, stage, version, new DateTime())

          dropCopy(datasetName, copyInfo, expectedLifecycleStage)
      }
    }
  }

  def dropWorkingCopy(datasetName: String, latest: DatasetCopy): Unit = {
    checkStage(LifecycleStage.Unpublished, latest.stage)

    if (latest.copyNumber < 2) {
      throw InvalidStateBeforeEvent("Cannot drop initial working copy")
    }

    logWorkingCopyDropped(datasetName, latest.copyNumber)
    client.deleteDatasetCopy(datasetName, latest.copyNumber, refresh = false)
    client.deleteColumnValuesByCopyNumber(datasetName, latest.copyNumber, refresh = false)
    client.deleteColumnMapsByCopyNumber(datasetName, latest.copyNumber, refresh = false)
    logRefreshRequest()
    client.refresh()
  }
}
