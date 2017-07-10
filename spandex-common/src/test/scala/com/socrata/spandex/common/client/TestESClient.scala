package com.socrata.spandex.common.client

import java.nio.file.Files
import scala.util.Try

import org.apache.commons.io.FileUtils
import org.elasticsearch.client.{Client, Requests}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.node.Node

import com.socrata.spandex.common.client.Queries._
import com.socrata.spandex.common.client.ResponseExtensions._
import com.socrata.spandex.common.client.SpandexElasticSearchClient._
import com.socrata.spandex.common.ElasticSearchConfig

class TestESClient(testSuiteName: String)
  extends SpandexElasticSearchClient("", 0, "", testSuiteName, 10000, 60000) {

  val tempDataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  val testSettings = Settings.builder()
    .put("cluster.name", testSuiteName)
    .put("path.data", tempDataDir.toString)
    .put("path.home", "target/elasticsearch")
    .put("transport.type", "local")
    .put("http.enabled", "false")
    .build

  val node = new Node(testSettings).start()

  override val client: Client = node.client()

  SpandexElasticSearchClient.ensureIndex(testSuiteName, this)

  override def close(): Unit = {
    deleteIndex()
    node.close()

    Try { // don't care if cleanup succeeded or failed
      FileUtils.forceDelete(tempDataDir)
    }

    super.close()
  }

  def deleteIndex(): Unit = {
    client.admin().indices().delete(Requests.deleteIndexRequest(testSuiteName))
  }

  def deleteAllDatasetCopies(): Unit =
    deleteByQuery(termQuery("_type", DatasetCopyType))

  def searchColumnMapsByDataset(datasetId: String): SearchResults[ColumnMap] =
    client.prepareSearch(testSuiteName)
      .setTypes(ColumnMapType)
      .setQuery(byDatasetIdQuery(datasetId))
      .execute.actionGet.results[ColumnMap]

  def searchColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnMap] =
    client.prepareSearch(testSuiteName)
      .setTypes(ColumnMapType)
      .setQuery(byCopyNumberQuery(datasetId, copyNumber))
      .execute.actionGet.results[ColumnMap]

  def searchFieldValuesByDataset(datasetId: String): SearchResults[FieldValue] = {
    val response = client.prepareSearch(testSuiteName)
      .setTypes(FieldValueType)
      .setQuery(byDatasetIdQuery(datasetId))
      .execute.actionGet
    response.results[FieldValue]
  }

  def searchFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(testSuiteName)
      .setTypes(FieldValueType)
      .setQuery(byCopyNumberQuery(datasetId, copyNumber))
      .execute.actionGet
    response.results[FieldValue]
  }

  def searchFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(testSuiteName)
      .setTypes(FieldValueType)
      .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
      .execute.actionGet
    response.results[FieldValue]
  }

  def searchFieldValuesByRowId(datasetId: String, copyNumber: Long, rowId: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(testSuiteName)
      .setTypes(FieldValueType)
      .setQuery(byRowIdQuery(datasetId, copyNumber, rowId))
      .execute.actionGet
    response.results[FieldValue]
  }

  def searchCopiesByDataset(datasetId: String): SearchResults[DatasetCopy] = {
    val response = client.prepareSearch(testSuiteName)
      .setTypes(DatasetCopyType)
      .setQuery(byDatasetIdQuery(datasetId))
      .execute.actionGet
    response.results[DatasetCopy]
  }
}

