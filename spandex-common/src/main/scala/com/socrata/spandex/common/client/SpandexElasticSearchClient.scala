package com.socrata.spandex.common.client

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary._
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.{DeleteRequestBuilder, DeleteResponse}
import org.elasticsearch.action.index.{IndexRequestBuilder, IndexResponse}
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.action.update.{UpdateRequestBuilder, UpdateResponse}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.rest.RestStatus.{CREATED, OK}
import org.elasticsearch.search.aggregations.AggregationBuilders.max
import org.elasticsearch.search.sort.{FieldSortBuilder, SortOrder}

import com.socrata.spandex.common.{ElasticSearchConfig, ElasticsearchClientLogger}
import com.socrata.spandex.common.client.ResponseExtensions._
import com.socrata.spandex.common.client.Queries._

// scalastyle:off number.of.methods
case class ElasticSearchResponseFailed(msg: String) extends Exception(msg)

// Setting refresh to true on ES write calls, because we always want to be
// sure that we're operating on the very latest copy. We are trading off some speed to get
// consistency.
// Without this, when we create a new working copy, more often than not the brand new
// dataset_copy document isn't indexed yet, and we perform all subsequent event operations
// on a stale copy.
// Caveats:
// - refresh=true only guarantees consistency on a single shard.
// - We aren't actually sure what the perf implications of running like this at production scale are.
// http://www.elastic.co/guide/en/elasticsearch/reference/1.x/docs-index_.html#index-refresh
class SpandexElasticSearchClient(
    host: String,
    port: Int,
    clusterName: String,
    indexName: String,
    dataCopyBatchSize: Int,
    dataCopyTimeout: Long)
  extends ElasticSearchClient(host, port, clusterName) {

  // scalastyle:ignore import.grouping
  import SpandexElasticSearchClient._

  private[this] def checkForFailures(response: ActionResponse): Unit = response match {
    case i: IndexResponse =>
      if (!Set(OK, CREATED).contains(i.status())) throw ElasticSearchResponseFailed(
        s"${i.getType} doc with id ${i.getId} was not successfully indexed; received status ${i.status()}")
    case u: UpdateResponse =>
    // No op - UpdateResponse doesn't have any useful state to check
    case d: DeleteResponse =>
    // No op - we don't care to throw an exception if d.isFound is false,
    // since that means the document is effectively deleted.
    case b: BulkResponse =>
      if (b.hasFailures) {
        throw new ElasticSearchResponseFailed(s"Bulk response contained failures: " +
          b.buildFailureMessage())
      }
    case _ =>
      throw new NotImplementedError(s"Haven't implemented failure check for ${response.getClass.getSimpleName}")
  }

  def refreshPolicy(refresh: Boolean): RefreshPolicy = if (refresh) RefreshPolicy.IMMEDIATE else RefreshPolicy.NONE

  def refresh(): Unit = client.admin().indices().prepareRefresh(indexName).execute.actionGet

  def indexExists: Boolean = {
    logIndexExistsRequest(indexName)
    val request = client.admin.indices.exists(new IndicesExistsRequest(indexName))
    val result = request.actionGet.isExists
    logIndexExistsResult(indexName, result)
    result
  }

  def putColumnMap(columnMap: ColumnMap, refresh: Boolean): Unit = {
    val source = JsonUtil.renderJson(columnMap)
    val request = client.prepareIndex(indexName, ColumnMapType, columnMap.docId)
      .setSource(source, XContentType.JSON)
      .setRefreshPolicy(refreshPolicy(refresh))
    logColumnMapIndexRequest(columnMap.docId, source)
    val response = request.execute.actionGet
    checkForFailures(response)
  }

  def columnMap(datasetId: String, copyNumber: Long, userColumnId: String): Option[ColumnMap] = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    val response = client.prepareGet(indexName, ColumnMapType, id).execute.actionGet
    response.result[ColumnMap]
  }

  def deleteColumnMap(datasetId: String, copyNumber: Long, userColumnId: String): Unit = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    checkForFailures(client.prepareDelete(indexName, ColumnMapType, id).execute.actionGet)
  }

  def deleteColumnMapsByDataset(datasetId: String): Unit =
    deleteByQuery(byDatasetIdQuery(datasetId), Seq(ColumnMapType))

  // We don't expect the number of column maps to exceed dataCopyBatchSize.
  // As of April 2015 the widest dataset is ~1000 cols wide.
  def searchLotsOfColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnMap] =
    client.prepareSearch(indexName)
      .setTypes(ColumnMapType)
      .setQuery(byCopyNumberQuery(datasetId, copyNumber))
      .setSize(dataCopyBatchSize)
      .execute.actionGet.results[ColumnMap]

  def deleteColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): Unit =
    deleteByQuery(byCopyNumberQuery(datasetId, copyNumber), Seq(ColumnMapType))

  def indexFieldValue(fieldValue: FieldValue, refresh: Boolean): Boolean = {
    if (fieldValue.isNonEmpty) {
      checkForFailures(fieldValueIndexRequest(fieldValue).setRefreshPolicy(refreshPolicy(refresh)).execute.actionGet)
      true
    } else {
      false
    }
  }

  def fieldValueIndexRequest(fieldValue: FieldValue): IndexRequestBuilder =
    client.prepareIndex(indexName, FieldValueType, fieldValue.docId)
      .setSource(JsonUtil.renderJson(fieldValue), XContentType.JSON)

  def fieldValueUpdateRequest(fieldValue: FieldValue): UpdateRequestBuilder = {
    client.prepareUpdate(indexName, FieldValueType, fieldValue.docId)
      .setDoc(JsonUtil.renderJson(fieldValue), XContentType.JSON)
      .setUpsert()
  }

  def fieldValueDeleteRequest(
      datasetId: String,
      copyNumber: Long,
      columnId: Long,
      rowId: Long)
    : DeleteRequestBuilder = {

    val docId = FieldValue.makeDocId(datasetId, copyNumber, columnId, rowId)
    client.prepareDelete(indexName, FieldValueType, docId)
  }

  // Yuk @ Seq[Any], but the number of types on ActionRequestBuilder is absurd.
  def sendBulkRequest(requests: Seq[Any], refresh: Boolean): Option[BulkResponse] = {
    if (requests.nonEmpty) {
      val baseRequest = client.prepareBulk().setRefreshPolicy(refreshPolicy(refresh))
      val bulkRequest = requests.foldLeft(baseRequest) { case (bulk, single) =>
        single match {
          case i: IndexRequestBuilder => bulk.add(i)
          case u: UpdateRequestBuilder => bulk.add(u)
          case d: DeleteRequestBuilder => bulk.add(d)
          case _ =>
            throw new UnsupportedOperationException(
              s"Bulk requests with ${single.getClass.getSimpleName} not supported")
        }
      }
      logBulkRequest(bulkRequest, refresh)
      val bulkResponse = bulkRequest.execute.actionGet
      checkForFailures(bulkResponse)
      Some(bulkResponse)
    } else { None }
  }

  /**
    * Scan + scroll response size is per shard, so we calculate the desired total size after counting primary shards.
    * See https://www.elastic.co/guide/en/elasticsearch/guide/1.x/scan-scroll.html#id-1.4.11.10.11.3
    *
    * Note: the value `indexName` could be an alias, and it's possible to iterate over returned settings and find
    * a concrete index with corresponding alias. But *index* vs *total* primary shards is almost the same number.
    */
  private def calculateScrollSize(desiredSize: Int): Int = {
    val clusterHealthRequest = client.admin.cluster.prepareHealth()
    val clusterHealthResponse = clusterHealthRequest.execute.actionGet
    val primaryShardCount = clusterHealthResponse.getActivePrimaryShards
    desiredSize / primaryShardCount
  }

  def deleteByQuery(
      queryBuilder: QueryBuilder,
      types: Seq[String] = Nil,
      refresh: Boolean = true)
     : Map[String, Int] = {

    logDeleteByQueryRequest(queryBuilder, types, refresh)
    val timeout = new TimeValue(dataCopyTimeout)
    val request = client.prepareSearch(indexName)
      .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
      .setQuery(queryBuilder)
      .setTypes(types: _*)
      .setScroll(timeout)
      .setSize(calculateScrollSize(calculateScrollSize(dataCopyBatchSize)))
    logSearchRequest(request, types)
    var response = request.execute.actionGet
    val resultCounts = mutable.Map[String, Int]()

    do {
      val batch = response.getHits.getHits.map { h =>
        client.prepareDelete(h.getIndex, h.getType, h.getId)
      }
      sendBulkRequest(batch, refresh = false).foreach { response =>
        resultCounts ++= response.deletions.map { case (k, v) => (k, resultCounts.getOrElse(k, 0) + v) }
      }

      response = client.prepareSearchScroll(response.getScrollId)
        .setScroll(timeout).execute.actionGet
      logSearchResponse(response)
    } while (response.getHits().getHits().length != 0)

    if (refresh) this.refresh()

    resultCounts.toMap
  }

  def copyFieldValues(from: DatasetCopy, to: DatasetCopy, refresh: Boolean): Unit = {
    logCopyFieldValuesRequest(from, to, refresh)
    val timeout = new TimeValue(dataCopyTimeout)
    val request = client.prepareSearch(indexName)
      .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
      .setTypes(FieldValueType)
      .setQuery(byCopyNumberQuery(from.datasetId, from.copyNumber))
      .setScroll(timeout)
      .setSize(calculateScrollSize(calculateScrollSize(dataCopyBatchSize)))
    logSearchRequest(request, Seq(FieldValueType))
    var response = request.execute.actionGet

    do {
      val batch = response.results[FieldValue].thisPage.map { src =>
        fieldValueIndexRequest(FieldValue(src.datasetId, to.copyNumber, src.columnId, src.rowId, src.value))
      }

      if (batch.nonEmpty) {
        sendBulkRequest(batch, refresh = false)
      }
      response = client.prepareSearchScroll(response.getScrollId)
        .setScroll(timeout).execute.actionGet
      logSearchResponse(response)
    } while (response.getHits().getHits().length != 0)

    // TODO : Guarantee refresh before read instead of after write
    if (refresh) {
      this.refresh()
    }
  }

  def deleteFieldValuesByDataset(datasetId: String): Unit =
    deleteByQuery(byDatasetIdQuery(datasetId), Seq(FieldValueType))

  def deleteFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): Unit =
    deleteByQuery(byCopyNumberQuery(datasetId, copyNumber), Seq(FieldValueType))

  def deleteFieldValuesByRowId(datasetId: String, copyNumber: Long, rowId: Long): Unit =
    deleteByQuery(byRowIdQuery(datasetId, copyNumber, rowId), Seq(FieldValueType))

  def deleteFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): Unit =
    deleteByQuery(byColumnIdQuery(datasetId, copyNumber, columnId), Seq(FieldValueType))

  def putDatasetCopy(
      datasetId: String,
      copyNumber: Long,
      dataVersion: Long,
      stage: LifecycleStage,
      refresh: Boolean)
    : Unit = {
    val id = DatasetCopy.makeDocId(datasetId, copyNumber)
    val source = JsonUtil.renderJson(DatasetCopy(datasetId, copyNumber, dataVersion, stage))
    val request = client.prepareIndex(indexName, DatasetCopyType, id)
      .setSource(source, XContentType.JSON)
      .setRefreshPolicy(refreshPolicy(refresh))
    logDatasetCopyIndexRequest(id, source)
    request.execute.actionGet
  }

  def updateDatasetCopyVersion(datasetCopy: DatasetCopy, refresh: Boolean): Unit = {
    val source = JsonUtil.renderJson(datasetCopy)
    val request = client.prepareUpdate(indexName, DatasetCopyType, datasetCopy.docId)
      .setDoc(source, XContentType.JSON)
      .setUpsert()
      .setRefreshPolicy(refreshPolicy(refresh))
    logDatasetCopyUpdateRequest(datasetCopy, source)
    val response = request.execute.actionGet
    checkForFailures(response)
  }

  def datasetCopyLatest(datasetId: String, stage: Option[Stage] = None): Option[DatasetCopy] = {
    val latestCopyPlaceholder = "latest_copy"
    val query = byDatasetIdAndOptionalStageQuery(datasetId, stage)

    val request = client.prepareSearch(indexName)
      .setTypes(DatasetCopyType)
      .setQuery(query)
      .setSize(1)
      .addSort(SpandexFields.CopyNumber, SortOrder.DESC)
      .addAggregation(max(latestCopyPlaceholder).field(SpandexFields.CopyNumber))
    logDatasetCopySearchRequest(request)

    val response = request.execute.actionGet
    val results = response.results[DatasetCopy]
    logDatasetCopySearchResults(results)
    results.thisPage.headOption
  }

  def datasetCopiesByStage(datasetId: String, stage: Stage): List[DatasetCopy] = {
    val query = byDatasetIdAndOptionalStageQuery(datasetId, Some(stage))

    val countRequest = client.prepareSearch(indexName)
      .setSize(0)
      .setTypes(DatasetCopyType)
      .setQuery(query)

    val count = countRequest.execute.actionGet

    val request = client.prepareSearch(indexName)
      .setTypes(DatasetCopyType)
      .setQuery(query)
      .setSize(count.getHits.totalHits.toInt)

    val response = request.execute.actionGet
    val datasetCopies = response.results[DatasetCopy]

    datasetCopies.thisPage.toList
  }

  def datasetCopy(datasetId: String, copyNumber: Long): Option[DatasetCopy] = {
    val id = DatasetCopy.makeDocId(datasetId, copyNumber)
    val request = client.prepareGet(indexName, DatasetCopyType, id)
    logDatasetCopyGetRequest(id)
    val response = request.execute.actionGet
    val datasetCopy = response.result[DatasetCopy]
    logDatasetCopyGetResult(datasetCopy)
    datasetCopy
  }

  def deleteDatasetCopy(datasetId: String, copyNumber: Long): Unit =
    deleteByQuery(byCopyNumberQuery(datasetId, copyNumber), Seq(DatasetCopyType))

  def deleteDatasetCopiesByDataset(datasetId: String): Unit =
    deleteByQuery(byDatasetIdQuery(datasetId), Seq(DatasetCopyType))

  private val allTypes = Seq(DatasetCopyType, ColumnMapType, FieldValueType)

  def deleteDatasetById(datasetId: String): Map[String, Int] =
    deleteByQuery(byDatasetIdQuery(datasetId), allTypes)

  def suggest(column: ColumnMap, size: Int, text: String): SearchResults[FieldValue] = {
    val fieldValue = if (text.length > 0) Some(text) else None
    val request = client.prepareSearch(indexName)
      .setSize(0)
      .setTypes(FieldValueType)
      .setQuery(byFieldValueAutocompleteAndCompositeIdQuery(fieldValue, column))
      .addAggregation(fieldValueTermsAggregation(size, orderByScore=fieldValue.isDefined))

    val aggKey = "values"
    request.execute.actionGet.results(aggKey)
  }
}

object SpandexElasticSearchClient extends ElasticsearchClientLogger {
  val DatasetCopyType = "dataset_copy"
  val ColumnMapType = "column_map"
  val FieldValueType = "field_value"

  def apply(config: ElasticSearchConfig): SpandexElasticSearchClient =
    new SpandexElasticSearchClient(
      config.host,
      config.port,
      config.clusterName,
      config.index,
      config.dataCopyBatchSize,
      config.dataCopyTimeout)

  def readSettings(): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(s"settings.json")
    Source.fromInputStream(stream).getLines().mkString("\n")
  }

  def readMapping(esType: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(s"mapping.$esType.json")
    Source.fromInputStream(stream).getLines().mkString("\n")
  }

  def ensureIndex(
      index: String,
      esClient: SpandexElasticSearchClient):
      Unit = {

    if (!esClient.indexExists) {
       try {
         logIndexCreateRequest(index)

         esClient.client.admin.indices.prepareCreate(index)
           .setSettings(readSettings, XContentType.JSON)
           .execute.actionGet

         // Add field_value mapping
         esClient.client.admin.indices.preparePutMapping(index)
           .setType(FieldValueType)
           .setSource(readMapping(FieldValueType), XContentType.JSON).execute.actionGet

         // Add column_map mapping
         esClient.client.admin.indices.preparePutMapping(index)
           .setType(ColumnMapType)
           .setSource(readMapping(ColumnMapType), XContentType.JSON).execute.actionGet

         // Add dataset_copy mapping
         esClient.client.admin.indices.preparePutMapping(index)
           .setType(DatasetCopyType)
           .setSource(readMapping(DatasetCopyType), XContentType.JSON).execute.actionGet
       } catch {
         // TODO: more error handling
         case e: ElasticsearchException =>
           logIndexAlreadyExists(index)
       }
    }
  }
}
