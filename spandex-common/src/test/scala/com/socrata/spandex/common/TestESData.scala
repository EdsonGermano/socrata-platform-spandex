package com.socrata.spandex.common

import com.socrata.spandex.common.client.SpandexElasticSearchClient

trait TestESData {
  case class IndexEntry(id: String, source: String)

  val datasets = Seq("primus.1234", "primus.9876")
  val columns = Seq("col1-1111", "col2-2222", "col3-3333")

  def config: SpandexConfig
  def client: SpandexElasticSearchClient

  def bootstrapData(): Unit = {
    for {
      ds     <- datasets
      copy   <- 1 to 2
      column <- columns
      row    <- 1 to 5
    } {
      val entry = makeEntry(ds, copy, column, row.toString)
      val response = client.client.prepareIndex(
        config.es.index, config.es.fieldValueMapping.mappingType, entry.id)
          .setSource(entry.source)
          .execute.actionGet
      assert(response.isCreated, s"failed to create ${entry.id}->$row")
    }

    // wait a sec to let elasticsearch index the documents
    Thread.sleep(1000) // scalastyle:ignore magic.number
  }

  def removeBootstrapData(): Unit = {
    datasets.foreach(client.deleteFieldValuesByDataset)
  }

  private def makeEntry(datasetId: String,
                        copyNumber: Long,
                        columnId: String,
                        rowId: String): IndexEntry = {
    val fieldValue = "data" + rowId
    val compositeId = s"$datasetId|$copyNumber|$columnId"
    val entryId = s"$compositeId|$rowId"

    val source =
      s"""{
        |  "dataset_id" : "$datasetId",
        |  "copy_number" : $copyNumber,
        |  "column_id" : "$columnId",
        |  "composite_id" : "$compositeId",
        |  "row_id" : $rowId,
        |  "value" : "$fieldValue"
        |}""".stripMargin

    IndexEntry(entryId, source)
  }
}
