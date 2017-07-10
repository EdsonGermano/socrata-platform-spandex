package com.socrata.spandex.common.client

import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.script.Script
import org.elasticsearch.search.aggregations.AggregationBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms

import com.socrata.datacoordinator.secondary.LifecycleStage

object Queries {
  def byDatasetIdQuery(datasetId: String): QueryBuilder = termQuery(SpandexFields.DatasetId, datasetId)

  def byDatasetIdAndStageQuery(datasetId: String, stage: LifecycleStage): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.DatasetId, datasetId))
      .must(termQuery(SpandexFields.Stage, stage.toString))

  def byCopyNumberQuery(datasetId: String, copyNumber: Long): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.DatasetId, datasetId))
      .must(termQuery(SpandexFields.CopyNumber, copyNumber))

  def byColumnIdQuery(datasetId: String, copyNumber: Long, columnId: Long): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.DatasetId, datasetId))
      .must(termQuery(SpandexFields.CopyNumber, copyNumber))
      .must(termQuery(SpandexFields.ColumnId, columnId))

  def byCompositeIdQuery(column: ColumnMap): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.CompositeId, column.compositeId))

  def byRowIdQuery(datasetId: String, copyNumber: Long, rowId: Long): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.DatasetId, datasetId))
      .must(termQuery(SpandexFields.CopyNumber, copyNumber))
      .must(termQuery(SpandexFields.RowId, rowId))

  def byDatasetIdAndOptionalStageQuery(datasetId: String, stage: Option[Stage]): QueryBuilder =
    stage match {
      case Some(n @ Number(_)) => throw new IllegalArgumentException(s"cannot request latest copy for stage = $n")
      case Some(Unpublished) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Unpublished)
      case Some(Published) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Published)
      case Some(Snapshotted) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Snapshotted)
      case Some(Discarded) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Discarded)
      case _ => byDatasetIdQuery(datasetId)
    }

  def byFieldValueAutocompleteQuery(fieldValue: Option[String]): QueryBuilder =
    fieldValue match {
      case Some(value) => matchQuery(SpandexFields.ValueAutocomplete, value)
      case None => matchAllQuery()
    }

  def byFieldValueAutocompleteAndCompositeIdQuery(fieldValue: Option[String], column: ColumnMap): QueryBuilder =
    boolQuery()
      .filter(byCompositeIdQuery(column))
      .must(byFieldValueAutocompleteQuery(fieldValue))

  def fieldValueTermsAggregation(
      size: Int,
      aggKey: String = "values",
      orderByScore: Boolean = false)
    : AggregationBuilder = {

    val termsAgg = terms(aggKey).field(SpandexFields.Value).size(size)

    if (orderByScore) {
      termsAgg.subAggregation(
        max("max_score").script(new Script("_score"))
      ).order(Terms.Order.aggregation("max_score.value", false))
    } else {
      termsAgg.order(Terms.Order.count(false))
    }
  }
}
