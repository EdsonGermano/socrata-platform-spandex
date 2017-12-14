package com.socrata.spandex.secondary

import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.secondary.Row
import com.socrata.soql.types.{SoQLText, SoQLValue}
import org.elasticsearch.action.ActionRequestBuilder

import com.socrata.spandex.common.client.{ColumnValue, ScoredResult, SpandexElasticSearchClient}
import com.socrata.spandex.secondary.RowOpsHandler.{columnValueFromDatum, columnValuesForRow}

class RowOpsHandler(client: SpandexElasticSearchClient, batchSize: Int) extends SecondaryEventLogger {
  def go(datasetName: String, copyNumber: Long, ops: Seq[Operation]): Unit = {
    // If there are deletes, we need the dataset's column IDs. If not, save the call to ES.
    val columnIds =
      if (ops.exists(_.isInstanceOf[Delete])) {
      client.searchLotsOfColumnMapsByCopyNumber(datasetName, copyNumber)
        .thisPage.map { case ScoredResult(columnMap, _) => columnMap.systemColumnId }
      } else {
        Seq.empty
      }

    val columnValues: Seq[ColumnValue] = ops.flatMap { op =>
      logRowOperation(op)

      // TODO: determine when data is None in the case of updates or deletions
      op match {
        case insert: Insert =>
          columnValuesForRow(datasetName, copyNumber, insert.data)
        case update: Update =>
          // decrement old column values
          val deletes = update.oldData.map { data =>
            columnValuesForRow(datasetName, copyNumber, data, isInsertion = false)
          }.getOrElse(List.empty)

          // increment new column values
          val inserts = columnValuesForRow(datasetName, copyNumber, update.data)
          deletes ++ inserts
        case delete: Delete =>
          // decrement deleted column values
          delete.oldData.map(data =>
            columnValuesForRow(datasetName, copyNumber, data, isInsertion = false)
          ).getOrElse(List.empty)
        case _ => throw new UnsupportedOperationException(s"Row operation ${op.getClass.getSimpleName} not supported")
      }
    }

    columnValues.grouped(batchSize).foreach { batch =>
      client.putColumnValues(datasetName, copyNumber, batch)
    }
  }
}

object RowOpsHandler {
  def apply(client: SpandexElasticSearchClient, batchSize: Int): RowOpsHandler =
    new RowOpsHandler(client, batchSize)

  private def columnValuesForRow(
      datasetName: String,
      copyNumber: Long,
      data: Row[SoQLValue],
      isInsertion: Boolean = true)
    : Seq[ColumnValue] = {
    data.toSeq.collect {
      // Spandex only stores text columns; other column types are a no op
      case (id, value: SoQLText) =>
        val count = if (isInsertion) 1L else -1L
        columnValueFromDatum(datasetName, copyNumber, (id, value), count)
    }
  }

  def columnValueFromDatum(
      datasetName: String,
      copyNumber: Long,
      datum: (ColumnId, SoQLText),
      count: Long = 1)
    : ColumnValue = datum match {
    case (id, value) => ColumnValue(
      // NOTE: a cluster side analysis char_filter doesn't catch this one character in time.
      // TODO: determine if this is truly necessary
      datasetName, copyNumber, id.underlying, value.value.replaceAll("\u001f", ""), count
    )
  }
}
