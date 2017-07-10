package com.socrata.spandex.common

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest
import scala.collection.JavaConverters._
import scala.collection.mutable

import com.socrata.datacoordinator.secondary.LifecycleStage

import com.socrata.spandex.common.client.{ColumnMap, DatasetCopy, FieldValue, TestESClient}
import com.socrata.spandex.common.client.SpandexElasticSearchClient
import com.socrata.spandex.common.client.Queries._

trait AnalyzerTest {
  val className = getClass.getSimpleName.toLowerCase

  protected lazy val client = new TestESClient(getClass.getSimpleName.toLowerCase)

  protected def analyzerBeforeAll(): Unit = {
    SpandexElasticSearchClient.ensureIndex(getClass.getSimpleName.toLowerCase, client)
  }

  protected def analyzerAfterAll(): Unit = {
    client.deleteIndex()
    client.close()
  }

  protected val ds = "ds.one"
  protected val copy = DatasetCopy(ds, 1, 42, LifecycleStage.Published)
  protected val col = ColumnMap(copy.datasetId, copy.copyNumber, 2, "column2")

  protected var docId = 10
  protected def index(value: String): Unit =
    index(FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, docId, value))

  protected def index(fv: FieldValue): Unit = {
    client.indexFieldValue(fv, refresh = true)
    docId += 1
  }

  protected def suggest(query: String): List[String] = {
    val response = client.suggest(col, 10, query)
    response.aggs.map(_.key).toList
  }
}
