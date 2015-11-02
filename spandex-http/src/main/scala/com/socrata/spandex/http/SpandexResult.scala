package com.socrata.spandex.http

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.spandex.common.client.{FieldValue, SearchResults}
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.suggest.Suggest.Suggestion
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry

import scala.collection.JavaConverters._
import scala.util.Try

case class SpandexOption(text: String, score: Option[Float])

object SpandexOption {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexOption]
}

case class SpandexResult(options: Seq[SpandexOption])

object SpandexResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexResult]

  def apply(response: Suggest): SpandexResult = {
    val suggest = response.getSuggestion[Suggestion[Entry]]("suggest")
    val entries = suggest.getEntries
    val options = entries.get(0).getOptions
    SpandexResult(options.asScala.map { a =>
      SpandexOption(a.getText.string(), Try{Some(a.getScore)}.getOrElse(None))
    })
  }

  def apply(response: SearchResults[FieldValue]): SpandexResult = {
    val hits = response.thisPage.map { src =>
      // TODO: aggregate with doc count
      SpandexOption(src.value, None)
    }
    val buckets = response.aggs.map { bc =>
      SpandexOption(bc.key, Some(bc.docCount))
    }
    SpandexResult(hits ++ buckets)
  }

  object Fields {
    private[this] def formatQuotedString(s: String) = "\"%s\"" format s
    val routeSuggest = "suggest"
    val routeSample = "sample"
    val paramDatasetId = "datasetId"
    val paramStageInfo = "stage"
    val paramUserColumnId = "userColumnId"
    val paramText = "text"
    val paramFuzz = "fuzz"
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
