package com.socrata.spandex.common

import com.socrata.spandex.common.client.ElasticSearchClient

trait TestESData {
  case class IndexEntry(id: String, source: String)

  val datasets = Seq("primus.1234", "primus.9876")
  val columns = Seq("col1-1111", "col2-2222", "col3-3333")

  def config: SpandexConfig
  def client: ElasticSearchClient

  def bootstrapData(): Unit = {
    for {
      ds     <- datasets
      copy   <- 1 to 2
      column <- columns
      value  <- 1 to 5
    } {
      val entry = makeEntry(ds, copy, column, value.toString)
      val response = client.client.prepareIndex(config.es.index, config.es.mappingType, entry.id)
        .setSource(entry.source)
        .execute.actionGet
      assert(response.isCreated, s"failed to create ${entry.id}->$value")
    }

    // wait a sec to let elasticsearch index the documents
    Thread.sleep(1000) // scalastyle:ignore magic.number
  }

  def removeBootstrapData(): Unit = {
    datasets.foreach(client.deleteByDataset)
  }

  private def makeEntry(datasetId: String,
                        copyId: Long,
                        columnId: String,
                        suffix: String): IndexEntry = {
    val fieldValue = "data" + suffix
    val compositeId = s"$datasetId|$copyId|$columnId"
    val entryId = s"$compositeId|$fieldValue"

    val source =
      s"""{
        |  "dataset_id" : "$datasetId",
        |  "copy_id" : $copyId,
        |  "column_id" : "$columnId",
        |  "composite_id" : "$compositeId",
        |  "value" : "$fieldValue"
        |}""".stripMargin

    IndexEntry(entryId, source)
  }
}