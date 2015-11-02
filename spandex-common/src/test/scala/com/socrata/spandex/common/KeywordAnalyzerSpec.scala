package com.socrata.spandex.common

import com.socrata.spandex.common.client._
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

class KeywordAnalyzerSpec extends FunSuiteLike with Matchers with AnalyzerTest with BeforeAndAfterAll {
  override val analyzerEnabled: Boolean = false
  override protected def beforeAll(): Unit = analyzerBeforeAll()
  override protected def afterAll(): Unit = analyzerAfterAll()

  // relocated from SpandexElasticSearchClientSpec
  test("suggest is case insensitive and supports numbers and symbols") {
    val fool = FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, 42L, "fool")
    val food = FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, 43L, "FOOD")
    val date = FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, 44L, "04/2014")
    val sym  = FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, 45L, "@giraffe")

    index(fool)
    index(food)
    index(date)
    index(sym)

    val suggestions = suggest("foo")
    // Urmila is scratching her head about what size() represents,
    // if there are 2 items returned but size() == 1
    suggestions.length should be(2)
    suggestions should contain(food.value)
    suggestions should contain(fool.value)

    val suggestionsUpper = suggest("FOO")
    suggestionsUpper.length should be(2)
    suggestionsUpper should contain(food.value)
    suggestionsUpper should contain(fool.value)

    val suggestionsNum = suggest("0")
    suggestionsNum.length should be(1)
    suggestionsNum should contain(date.value)

    val suggestionsSym = suggest("@gir")
    suggestionsSym.length should be(1)
    suggestionsSym should contain(sym.value)
  }

  test("settings include custom analyzer") {
    val clusterState = client.client.admin().cluster().prepareState().setIndices(config.es.index)
      .execute().actionGet().getState
    val indexMetadata = clusterState.getMetaData.index(config.es.index)
    val fieldValueTypeMapping = indexMetadata.mapping("field_value").source.toString
    fieldValueTypeMapping should include("case_insensitive_keyword")
  }

  test("match: simple") {
    val expectedValue = "Supercalifragilisticexpialidocious"
    index(expectedValue)
    suggest("sup") should contain(expectedValue)
  }

  test("match: keyword") {
    val expectedValue = "The quick brown fox jumps over the lazy dog"
    index(expectedValue)
    suggest("the quick") should contain(expectedValue)
  }

  test("match: term") {
    val expectedValue = "Former President Abraham Lincoln"
    index(expectedValue)
    suggest("form") should contain(expectedValue)
    suggest("pres") should be('empty)
    suggest("abra") should be('empty)
    suggest("linc") should be('empty)
    suggest("former pres") should contain(expectedValue)
  }

  test("match: email") {
    val expectedValue = "we.are+awesome@socrata.com"
    index(expectedValue)
    suggest("we") should contain(expectedValue)
    suggest("are") should be('empty)
    suggest("awesome") should be('empty)
    suggest("socrata") should be('empty)
    suggest("com") should be('empty)
    suggest("we.are+awesome") should contain(expectedValue)
  }

  test("match: url") {
    val expectedValue = "https://lucene.rocks/completion-suggester.html"
    index(expectedValue)
    suggest("https") should contain(expectedValue)
    suggest("lucene") should be('empty)
    suggest("rocks") should be('empty)
    suggest("completion") should be('empty)
    suggest("suggester") should be('empty)
    suggest("html") should be('empty)
    suggest("https://lucene.rocks") should contain(expectedValue)
  }

  test("completion pre analyzer is disabled") {
    val value = "A phrase is a group of related words that does not include a subject and verb. (If the group of related words does contain a subject and verb, it is considered a clause.) There are several different kinds of phrases."
    val expectedTokens = List(value)
    val tokens = CompletionAnalyzer.analyze(value)
    tokens should equal(expectedTokens)
  }

  test("match: non-english unicode") {
    val expectedValue = "愛"

    val tokens = CompletionAnalyzer.analyze(expectedValue)
    tokens should contain(expectedValue)

    index(expectedValue)
    suggest(expectedValue) should contain(expectedValue)
  }

  test("match: money") {
    val expectedValue = "$500 and under"
    val search = "$"

    val tokens = CompletionAnalyzer.analyze(expectedValue)
    tokens should contain(expectedValue)

    index(expectedValue)
    suggest(search) should contain(expectedValue)
  }

  test("match: ampersand") {
    val expectedValue = "AT&T Mobility"
    val search = "AT&T"

    val tokens = CompletionAnalyzer.analyze(expectedValue)
    tokens should contain(expectedValue)

    index(expectedValue)
    suggest(search) should contain(expectedValue)
  }

  test("match: forward slash") {
    val expectedValue = "ANDREA H/ARTHUR D HARRIS"
    val search = "ANDREA H/ARTHUR"

    val tokens = CompletionAnalyzer.analyze(expectedValue)
    tokens should contain(expectedValue)

    index(expectedValue)
    suggest(search) should contain(expectedValue)
  }

  test("not match: dot") {
    val value = "The quick and the dead"
    index(value)
    suggest(".") should be('empty)
  }
}
