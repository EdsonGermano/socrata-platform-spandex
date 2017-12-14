package com.socrata.spandex.common

import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilder

import com.socrata.spandex.common.client.{DatasetCopy, SpandexElasticSearchClient, SearchResults}

trait ElasticsearchClientLogger extends Logging {
  this: SpandexElasticSearchClient =>

  def queryToString(req: Any): String = req.toString.replaceAll("\\s+", " ")

  def logClientConnected(): Unit = {
    val response = client.admin().cluster().health(new ClusterHealthRequest()).actionGet()
    val clusterName = response.getClusterName
    val settings = client.settings().toDelimitedString(',')

    logger.debug(s"connected elasticsearch client to cluster $clusterName with $settings")
  }

  def logClientHealthCheckStatus(): Unit = {
    val status = client.admin().cluster().prepareHealth().get().toString
    logger.debug(s"elasticsearch cluster healthcheck $status")
  }

  def logIndexExistsRequest(index: String): Unit =
    logger.debug(s"does index '$index' exist?")

  def logIndexExistsResult(index: String, result: Boolean): Unit =
    logger.debug(s"index '$index' exists was '$result'")

  def logIndexCreateRequest(index: String): Unit =
    logger.info(s"creating index $index")

  def logIndexAlreadyExists(index: String): Unit =
    logger.info(s"actually that index ($index) already exists")

  def logBulkRequest(request: BulkRequestBuilder, refresh: Boolean): Unit =
    logger.debug(s"sending bulk request of size ${request.numberOfActions} with refresh=$refresh")

  def logDeleteByQueryRequest(queryBuilder: QueryBuilder, types: Seq[String], refresh: Boolean): Unit =
    logger.debug(
      s"delete by query ${queryToString(queryBuilder)} on types=${types.toString} with refresh=${refresh.toString}")

  def logSearchRequest(search: SearchRequestBuilder, types: Seq[String]): Unit =
    logger.debug(s"search request ${queryToString(search)} on types=${types.toString()}")

  def logSearchScrollRequest(scrollId: String, timeout: String): Unit =
    logger.trace(s"search scroll request id=$scrollId timeout=$timeout")

  def logCopyColumnValuesRequest(from: DatasetCopy, to: DatasetCopy, refresh: Boolean): Unit =
    logger.debug(s"copy column_values from=$from to=$to refresh=$refresh")

  def logDatasetCopyIndexRequest(id: String, source: String): Unit =
    logger.debug(s"executing index dataset copy on id=$id with $source")

  def logDatasetCopyUpdateRequest(datasetCopy: DatasetCopy, source: String): Unit =
    logger.debug(s"executing update dataset copy version request on id=${datasetCopy.docId} with $source")

  def logDatasetCopySearchRequest(request: SearchRequestBuilder): Unit =
    logger.debug(s"executing elasticsearch search request ${queryToString(request)}")

  def logDatasetCopySearchResults(results: SearchResults[DatasetCopy]): Unit =
    logger.debug(s"received dataset copies ${results.thisPage.mkString(", ")}")

  def logDatasetCopyGetRequest(id: String): Unit =
    logger.debug(s"executing elasticsearch get request for dataset copy id = $id")

  def logDatasetCopyGetResult(result: Option[DatasetCopy]): Unit =
    logger.debug(s"received dataset copy $result")

  def logColumnMapIndexRequest(id: String, source: String): Unit =
    logger.debug(s"executing index column map on id=$id with $source")

  def logSearchResponse(response: SearchResponse): Unit =
    logger.debug(s"received ${response.getHits.getHits.length} hits out of ${response.getHits.totalHits} total")
}
