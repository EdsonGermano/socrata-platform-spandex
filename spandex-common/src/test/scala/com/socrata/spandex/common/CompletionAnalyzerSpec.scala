package com.socrata.spandex.common

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.spandex.common.client._
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

class CompletionAnalyzerSpec extends FunSuiteLike with Matchers with AnalyzerTest with BeforeAndAfterAll {
  override val analyzerEnabled: Boolean = true
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
    fieldValueTypeMapping should include("case_insensitive_word")
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

  test("completion pre analyzer creates expected tokens") {
    val value = "Former President Abraham Lincoln"
    val expectedTokens = List(
      "former president abraham lincoln",
      "president abraham lincoln",
      "abraham lincoln",
      "lincoln").reverse
    val tokens = CompletionAnalyzer.analyze(value)
    tokens should equal(expectedTokens)
  }

  test("field value json includes value input/output elements") {
    val value = "Donuts and Coconuts"
    val expectedInputTokens = List("donuts", "and", "coconuts")
    val fv = FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, 11, value)
    val json = JsonUtil.renderJson(fv)
    json should include("\"input\":[")
    json should include("\"output\":")
  }

  test("match: term") {
    val expectedValue = "Former President Abraham Lincoln"
    index(expectedValue)
    suggest("form") should contain(expectedValue)
    suggest("pres") should contain(expectedValue)
    suggest("abra") should contain(expectedValue)
    suggest("linc") should contain(expectedValue)
    suggest("abraham linc") should contain(expectedValue)
  }

  test("match: email") {
    val expectedValue = "we.are+awesome@socrata.com"

    val tokens = CompletionAnalyzer.analyze(expectedValue)
    tokens should contain("we are awesome socrata com")

    index(expectedValue)
    suggest("we") should contain(expectedValue)
    suggest("are") should contain(expectedValue)
    suggest("awesome") should contain(expectedValue)
    suggest("socrata") should contain(expectedValue)
    suggest("com") should contain(expectedValue)
    suggest("socrata.com") should contain(expectedValue)
  }

  test("match: url") {
    val expectedValue = "https://lucene.rocks/auto-suggester.html"

    val tokens = CompletionAnalyzer.analyze(expectedValue)
    tokens should contain("https lucene rocks auto suggester html")

    index(expectedValue)
    suggest("https") should contain(expectedValue)
    suggest("lucene") should contain(expectedValue)
    suggest("rocks") should contain(expectedValue)
    suggest("auto") should contain(expectedValue)
    suggest("suggester") should contain(expectedValue)
    suggest("html") should contain(expectedValue)
    suggest("lucene rocks") should contain(expectedValue)
  }

  test("completion pre analyzer length limits don't cutoff terms") {
    // a fictional dish mentioned in Aristophanes' comedy Assemblywomen
    val value = "Lopadotemachoselachogaleokranioleipsanodrimhypotrimmatosilphioparaomelitokatakechymenokichlepikossyphophattoperisteralektryonoptekephalliokigklopeleiolagoiosiraiobaphetraganopterygon"
    val expectedInputValue = value.toLowerCase
    val tokens = CompletionAnalyzer.analyze(value)
    tokens.head should equal(expectedInputValue)
  }

  test("completion pre analyzer limit input string length") {
    val value = "It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife. However little known the feelings or views of such a man may be on his first entering a neighbourhood, this truth is so well fixed in the minds of the surrounding families, that he is considered the rightful property of some one or other of their daughters. My dear Mr. Bennet, said his lady to him one day, have you heard that Netherfield Park is let at last? Mr. Bennet replied that he had not. But it is, returned she; for Mrs. Long has just been here, and she told me all about it. Mr. Bennet made no answer. Do you not want to know who has taken it? cried his wife impatiently. You want to tell me, and I have no objection to hearing it."
    // the config value 64 limits to -------------------------------------------^
    //                plus 14 spaces ---- - - - - - - - - - - - - - - - - - - - - - - - - - ---^
    //                                                              through the end of a term ---^
    val expectedInputValues = Seq(
      "good",
      "a good",
      "of a good",
      "possession of a good",
      "in possession of a good",
      "man in possession of a good",
      "single man in possession of a good",
      "a single man in possession of a good",
      "that a single man in possession of a good",
      "acknowledged that a single man in possession",
      "universally acknowledged that a single",
      "truth universally acknowledged that",
      "a truth universally acknowledged that",
      "is a truth universally acknowledged that",
      "it is a truth universally acknowledged")
    // the config value 32 limits to -^ plus spaces, and a little more to preserve terms
    CompletionAnalyzer.analyze(value) should equal(expectedInputValues)
  }

  test("match: non-english unicode") {
    val expectedValue = "愛" // scalastyle:ignore

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
    val expectedValue = "at&t mobility"
    val search = "at&t"

    val tokens = CompletionAnalyzer.analyze(expectedValue)
    tokens should contain(expectedValue)

    index(expectedValue)
    suggest(search) should contain(expectedValue)
  }

  test("match: forward slash") {
    val value = "andrea h/arthur d harris"
    val expectedValue = "andrea h arthur d harris"
    val search = "h/arthur"

    val tokens = CompletionAnalyzer.analyze(value)
    println(tokens)
    tokens should contain(expectedValue)

    index(value)
    suggest(search) should contain(value)
  }

  test("not match: dot") {
    val value = "The quick and the dead"
    index(value)
    suggest(".") should be('empty)
  }
}
