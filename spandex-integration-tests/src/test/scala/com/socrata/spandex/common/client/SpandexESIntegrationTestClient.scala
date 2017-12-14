package com.socrata.spandex.common.client

import scala.language.implicitConversions
import scala.util.control.NonFatal
import java.net.InetAddress

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders.{boolQuery, termQuery}
import org.elasticsearch.transport.client.PreBuiltTransportClient

import com.socrata.spandex.common.TestData
import Queries._
import ResponseExtensions._
import SpandexElasticSearchClient.{ColumnType, ColumnValueType, DatasetCopyType}

class SpandexESIntegrationTestClient(val client: SpandexElasticSearchClient) {
  def ensureIndex(): Unit =
    SpandexElasticSearchClient.ensureIndex(client.indexName, client)

  private val healthTimeoutMillis = 2000
  private val acceptableClusterHealthStatuses = List(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW)

  def isConnected: Boolean = try {
    val status = client.client.admin().cluster().prepareHealth()
      .execute.actionGet(healthTimeoutMillis).getStatus
    acceptableClusterHealthStatuses.contains(status)
  } catch {
    case NonFatal(e) =>
      false
  }

  def bootstrapData(testData: TestData): Unit = {
    // Create 2 datasets with 3 copies each:
    // - an old snapshot copy
    // - the most recent published copy
    // - a working copy
    testData.datasets.foreach { ds =>
      testData.copies(ds).foreach { copy =>
        client.putDatasetCopy(ds, copy.copyNumber, copy.version, copy.stage, refresh = true)

        testData.columns(ds, copy).foreach { col =>
          client.putColumnMap(
            ColumnMap(ds, copy.copyNumber, col.systemColumnId, col.userColumnId),
            refresh = true
          )

          testData.rows(col).foreach(indexColumnValue)
        }
      }
    }
  }

  def removeBootstrapData(testData: TestData): Unit =
    testData.datasets.foreach { d =>
      client.deleteColumnValuesByDataset(d, false)
      client.deleteColumnMapsByDataset(d, false)
      client.deleteDatasetCopiesByDataset(d, false)
    }

  def deleteIndex(): Unit =
    client.client.admin().indices().delete(new DeleteIndexRequest(client.indexName)).actionGet()
  
  def deleteAllDatasetCopies(): Unit =
    client.deleteByQuery(termQuery("_type", DatasetCopyType))

  def searchColumnValuesByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnValue] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnValueType)
      .setQuery(byCopyNumberQuery(datasetId, copyNumber))
      .execute.actionGet
    response.results[ColumnValue]()
  }

  def searchColumnValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): SearchResults[ColumnValue] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnValueType)
      .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
      .execute.actionGet
    response.results[ColumnValue]()
  }

  def searchColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnMap] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnType)
      .setQuery(byCopyNumberQuery(datasetId, copyNumber))
      .execute.actionGet
    response.results[ColumnMap]()
  }

  def indexColumnValue(columnValue: ColumnValue): Unit =
    client.indexColumnValues(Seq(columnValue), refresh = true)

  def fetchColumnValueByDocid(columnValId: String): ColumnValue = {
    val response = client.client.prepareGet(client.indexName, ColumnValueType, columnValId)
      .execute.actionGet
    response.result[ColumnValue].get
  }

  def searchColumnValuesByDataset(datasetId: String): SearchResults[ColumnValue] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnValueType)
      .setQuery(byDatasetIdQuery(datasetId))
      .execute.actionGet
    response.results[ColumnValue]()
  }

  def searchColumnMapsByDataset(datasetId: String): SearchResults[ColumnMap] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnType)
      .setQuery(byDatasetIdQuery(datasetId))
      .execute.actionGet
    response.results[ColumnMap]()
  }

  def searchCopiesByDataset(datasetId: String): SearchResults[DatasetCopy] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(DatasetCopyType)
      .setQuery(byDatasetIdQuery(datasetId))
      .execute.actionGet
    response.results[DatasetCopy]()
  }

  def fetchCountForColumnValue(datasetId: String, value: String): Option[Long] = {
    val query = boolQuery().must(byDatasetIdQuery(datasetId)).must(termQuery("value", value))

    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnValueType)
      .setQuery(query)
      .execute.actionGet

    response.results[ColumnValue]().thisPage.map(_.result.count).headOption
  }
}

object SpandexESIntegrationTestClient {
  def apply(
      host: String,
      port: Int,
      clusterName: String,
      indexName: String,
      dataCopyBatchSize: Int,
      dataCopyTimeout: Long)
    : SpandexESIntegrationTestClient = {

    val settings = Settings.builder()
      .put("cluster.name", clusterName)
      .put("client.transport.sniff", true)
      .build()

    val transportAddress = new InetSocketTransportAddress(InetAddress.getByName(host), port)
    val transportClient: Client = new PreBuiltTransportClient(settings).addTransportAddress(transportAddress)

    val client = new SpandexElasticSearchClient(transportClient, indexName, dataCopyBatchSize, dataCopyTimeout)

    new SpandexESIntegrationTestClient(client)
  }

  implicit def underlying(client: SpandexESIntegrationTestClient): SpandexElasticSearchClient =
    client.client
}
