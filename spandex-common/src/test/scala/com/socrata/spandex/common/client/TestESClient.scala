package com.socrata.spandex.common.client

import java.nio.file.Files

import com.socrata.spandex.common.client.ResponseExtensions._
import com.socrata.spandex.common.{ElasticSearchConfig, SpandexBootstrap}
import org.apache.commons.io.FileUtils
import org.elasticsearch.client.{Client, Requests}
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.node.NodeBuilder._

import scala.util.Try

class TestESClient(config: ElasticSearchConfig, local: Boolean = true) extends SpandexElasticSearchClient(config) {
  val tempDataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  val testSettings = ImmutableSettings.settingsBuilder()
    .put(settings)
    .put("discovery.zen.ping.multicast.enabled", false)
    .put("path.data", tempDataDir.toString)
    .build

  val node = nodeBuilder().settings(testSettings).local(local).node()

  override val client: Client = node.client()

  SpandexBootstrap.ensureIndex(config, this)

  override def close(): Unit = {
    deleteIndex()
    node.close()

    Try { // don't care if cleanup succeeded or failed
      FileUtils.forceDelete(tempDataDir)
    }

    super.close()
  }

  def deleteIndex(): Unit = {
    client.admin().indices().delete(Requests.deleteIndexRequest(config.index))
  }

  def deleteAllDatasetCopies(): Unit =
    deleteByQuery(termQuery("_type", config.datasetCopyMapping), Seq(config.datasetCopyMapping.mappingType))

  def searchColumnMapsByDataset(datasetId: String): SearchResults[ColumnMap] =
    client.prepareSearch(config.index)
          .setTypes(config.columnMapMapping.mappingType)
          .setQuery(byDatasetIdQuery(datasetId))
          .execute.actionGet.results[ColumnMap]

  def searchColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnMap] =
    client.prepareSearch(config.index)
          .setTypes(config.columnMapMapping.mappingType)
          .setQuery(byCopyNumberQuery(datasetId, copyNumber))
          .execute.actionGet.results[ColumnMap]

  def searchFieldValuesByDataset(datasetId: String): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byDatasetIdQuery(datasetId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def searchFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byCopyNumberQuery(datasetId, copyNumber))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def searchFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def searchFieldValuesByRowId(datasetId: String, copyNumber: Long, rowId: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byRowIdQuery(datasetId, copyNumber, rowId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def searchCopiesByDataset(datasetId: String): SearchResults[DatasetCopy] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.datasetCopyMapping.mappingType)
                         .setQuery(byDatasetIdQuery(datasetId))
                         .execute.actionGet
    response.results[DatasetCopy]
  }
}
