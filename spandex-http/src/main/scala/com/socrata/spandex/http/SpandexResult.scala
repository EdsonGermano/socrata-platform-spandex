package com.socrata.spandex.http

import scala.collection.JavaConverters._
import scala.util.Try

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.typesafe.scalalogging.slf4j.Logging
import com.socrata.spandex.common.client.BucketKeyVal

import com.socrata.spandex.common.client.{FieldValue, SearchResults, SpandexFields}

case class SpandexOption(text: String, score: Option[Float])

object SpandexOption {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexOption]
}

case class SpandexResult(options: Seq[SpandexOption])

object SpandexResult extends Logging {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexResult]

  def apply(results: SearchResults[FieldValue]): SpandexResult =
    SpandexResult(
      results.aggs.map { case BucketKeyVal(key, value) =>
        SpandexOption(key, Some(value.toFloat))
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
