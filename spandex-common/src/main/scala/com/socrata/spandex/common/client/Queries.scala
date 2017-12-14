package com.socrata.spandex.common.client

import scala.collection.JavaConverters._

import org.elasticsearch.common.lucene.search.function.CombineFunction
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilder, TermQueryBuilder}
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders
import org.elasticsearch.script.{Script, ScriptType}

import com.socrata.datacoordinator.secondary.LifecycleStage

object Queries {
  def byDatasetIdQuery(datasetId: String): TermQueryBuilder =
    termQuery(SpandexFields.DatasetId, datasetId)

  def byDatasetIdAndStageQuery(datasetId: String, stage: LifecycleStage): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.DatasetId, datasetId))
      .filter(termQuery(SpandexFields.Stage, stage.toString))

  def byCopyNumberQuery(datasetId: String, copyNumber: Long): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.DatasetId, datasetId))
      .filter(termQuery(SpandexFields.CopyNumber, copyNumber))

  def byColumnIdQuery(datasetId: String, copyNumber: Long, columnId: Long): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.DatasetId, datasetId))
      .filter(termQuery(SpandexFields.CopyNumber, copyNumber))
      .filter(termQuery(SpandexFields.ColumnId, columnId))

  def byCompositeIdQuery(column: ColumnMap): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.CompositeId, column.compositeId))

  def byRowIdQuery(datasetId: String, copyNumber: Long, rowId: Long): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.DatasetId, datasetId))
      .filter(termQuery(SpandexFields.CopyNumber, copyNumber))
      .filter(termQuery(SpandexFields.RowId, rowId))

  def byDatasetIdAndOptionalStageQuery(datasetId: String, stage: Option[Stage]): QueryBuilder =
    stage match {
      case Some(n @ Number(_)) => throw new IllegalArgumentException(s"cannot request latest copy for stage = $n")
      case Some(Unpublished) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Unpublished)
      case Some(Published) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Published)
      case Some(Snapshotted) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Snapshotted)
      case Some(Discarded) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Discarded)
      case _ => byDatasetIdQuery(datasetId)
    }

  def byColumnValueAutocompleteQuery(columnValue: Option[String]): QueryBuilder =
    columnValue match {
      case Some(value) => matchQuery(SpandexFields.ValueAutocomplete, value)
      case None => matchAllQuery()
    }

  def byColumnValueAutocompleteAndCompositeIdQuery(columnValue: Option[String], column: ColumnMap): BoolQueryBuilder =
    boolQuery()
      .filter(byCompositeIdQuery(column))
      .must(byColumnValueAutocompleteQuery(columnValue))

  def scoreByCountQuery(query: QueryBuilder): FunctionScoreQueryBuilder = {
    val script = new Script(ScriptType.INLINE, "painless", "doc['count'].value", Map.empty[String, Object].asJava)
    val fn = new FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(script))
    functionScoreQuery(query, Array(fn)).boostMode(CombineFunction.REPLACE)
  }

  def nonPositiveCountColumnValuesByCopyNumberQuery(datasetId: String, copyNumber: Long): BoolQueryBuilder =
    byCopyNumberQuery(datasetId, copyNumber).filter(rangeQuery(SpandexFields.Count).lte(0))
}
