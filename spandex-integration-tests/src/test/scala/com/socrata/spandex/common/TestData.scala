package com.socrata.spandex.common

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client._

trait TestData {
  val datasets = Seq("primus.1234", "primus.9876")

  def copies(dataset: String): Seq[DatasetCopy] = {
    val snapshot = DatasetCopy(dataset, 1, 5, LifecycleStage.Snapshotted) // scalastyle:ignore magic.number
    val published = DatasetCopy(dataset, 2, 10, LifecycleStage.Published) // scalastyle:ignore magic.number
    val workingCopy = DatasetCopy(dataset, 3, 15, LifecycleStage.Unpublished) // scalastyle:ignore magic.number
    Seq(snapshot, published, workingCopy).sortBy(_.copyNumber)
  }  

  def columns(dataset: String, copy: DatasetCopy): Seq[ColumnMap] =
    (1 to 3).map(column => ColumnMap(dataset, copy.copyNumber, column.toLong, s"col$column"))

  def makeRowData(col: Long, row: Long): String = s"data column $col row $row"

  def rows(col: ColumnMap): Seq[ColumnValue] =
    (1 to 5).map(rowId =>
      ColumnValue(
        col.datasetId, col.copyNumber, col.systemColumnId, makeRowData(col.systemColumnId, rowId.toLong), 1L)
    )
}
