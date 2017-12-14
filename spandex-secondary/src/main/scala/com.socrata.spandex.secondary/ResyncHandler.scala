package com.socrata.spandex.secondary

import com.rojoma.simplearm._
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLID, SoQLText, SoQLType, SoQLValue}
import com.socrata.spandex.common.client.{ColumnMap, SpandexElasticSearchClient}

case class ResyncHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  def go(datasetInfo: DatasetInfo,
         copyInfo: CopyInfo,
         schema: ColumnIdMap[ColumnInfo[SoQLType]],
         rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
         batchSize: Int): Unit = {
    logResync(datasetInfo.internalName, copyInfo.copyNumber)

    // Add dataset copy
    // Don't refresh ES during resync
    client.putDatasetCopy(datasetInfo.internalName,
      copyInfo.copyNumber,
      copyInfo.dataVersion,
      copyInfo.lifecycleStage,
      refresh = false)

    // Add column maps for text columns
    val textColumns =
      schema.toSeq.collect { case (id, info) if info.typ == SoQLText =>
        ColumnMap(datasetInfo.internalName, copyInfo.copyNumber, info)
      }

    // Don't refresh ES during resync
    textColumns.foreach(client.putColumnMap(_, refresh = false))

    // Add field values for text columns
    insertRows(datasetInfo, copyInfo, schema, rows, batchSize)
  }

  private[this] def insertRows(
      datasetInfo: DatasetInfo,
      copyInfo: CopyInfo,
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
      batchSize: Int) = {
    // Add column values for text columns
    for { iter <- rows } {
      val columnValues =
        for {
          row <- iter
          (id, value: SoQLText) <- row.iterator
        } yield {
          RowOpsHandler.columnValueFromDatum(
            datasetInfo.internalName, copyInfo.copyNumber, (id, value))
        }

      // Don't refresh ES during resync
      columnValues.grouped(batchSize).foreach { batch =>
        client.putColumnValues(datasetInfo.internalName, copyInfo.copyNumber, batch)
      }
    }
  }
}
