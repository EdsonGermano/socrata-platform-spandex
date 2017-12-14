package com.socrata.spandex.http

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.typesafe.scalalogging.slf4j.Logging

import com.socrata.spandex.common.client.{ColumnValue, ScoredResult, SearchResults}

case class SpandexOption(text: String, score: Option[Float])

object SpandexOption {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexOption]
}

case class SpandexResult(options: Seq[SpandexOption])

object SpandexResult extends Logging {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexResult]

  def apply(results: SearchResults[ColumnValue]): SpandexResult =
    SpandexResult(
      results.thisPage.map { case ScoredResult(ColumnValue(_, _, _, value, _), score) =>
        SpandexOption(value, Some(score))
      }
    )

  object Fields {
    private[this] def formatQuotedString(s: String) = "\"%s\"" format s
    val routeSuggest = "suggest"
    val routeSample = "sample"
    val paramDatasetId = "datasetId"
    val paramStageInfo = "stage"
    val paramUserColumnId = "userColumnId"
    val paramText = "text"
    val paramSize = "size"
    val options = "options"
    val optionsJson = formatQuotedString(options)
    val optionsEmptyJson = "\"%s\":[]" format options
    val text = "text"
    val textJson = formatQuotedString(text)
    val score = "score"
    val scoreJson = formatQuotedString(score)
  }
}
