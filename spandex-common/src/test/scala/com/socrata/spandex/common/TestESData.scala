package com.socrata.spandex.common

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client._

trait TestESData {
  case class IndexEntry(id: String, source: String)

  val datasets = Seq("primus.1234", "primus.9876")

  def copies(dataset: String): Seq[DatasetCopy] = {
    val snapshot    = DatasetCopy(dataset, 1, 5, LifecycleStage.Snapshotted)
    val published   = DatasetCopy(dataset, 2, 10, LifecycleStage.Published)
    val workingCopy = DatasetCopy(dataset, 3, 15, LifecycleStage.Unpublished)
    Seq(snapshot, published, workingCopy).sortBy(_.copyNumber)
  }

  def columns(dataset: String, copy: DatasetCopy): Seq[ColumnMap] = {
    for {column <- 1 to 3} yield {
      ColumnMap(dataset, copy.copyNumber, column, s"col$column")
    }
  }

  def makeRowData(col: Long, row: Long): String = s"data column $col row $row"
  def rows(col: ColumnMap): Seq[FieldValue] = {
    for {row <- 1 to 5} yield {
      FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, row, makeRowData(col.systemColumnId, row))
    }
  }

  def config: SpandexConfig
  def client: SpandexElasticSearchClient

  def bootstrapData(): Unit = {
    // Create 2 datasets with 3 copies each:
    // - an old snapshot copy
    // - the most recent published copy
    // - a working copy
    for {
      ds <- datasets
    } {
      for { copy <- copies(ds) } {
        client.putDatasetCopy(ds, copy.copyNumber, copy.version, copy.stage, refresh = true)

        for {col <- columns(ds, copy)} {
          client.putColumnMap(
            ColumnMap(ds, copy.copyNumber, col.systemColumnId, col.userColumnId),
            refresh = true)

          for {doc <- rows(col)} {
            client.indexFieldValue(doc, refresh = true)
          }
        }
      }
    }
  }

  def removeBootstrapData(): Unit = {
    datasets.foreach(client.deleteFieldValuesByDataset)
    datasets.foreach(client.deleteColumnMapsByDataset)
    datasets.foreach(client.deleteDatasetCopiesByDataset)
  }
}
