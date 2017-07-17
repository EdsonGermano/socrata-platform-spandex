package com.socrata.spandex.common

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.spandex.common.client._

import org.scalatest.Ignore

@Ignore
class SuggestionsSpec extends FunSuiteLike with Matchers with AnalyzerTest with BeforeAndAfterAll {
  // NOTE: these tests should be moved out into separate integration test suite
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

  test("suggest returns relevant matches when the query is a prefix string to an indexed field value") {
    val expectedValue = "Supercalifragilisticexpialidocious"
    index(expectedValue)
    suggest("sup") should contain(expectedValue)
  }

  test("suggest returns relevant matches when the query is prefix phrase to an indexed field value") {
    val expectedValue = "The quick brown fox jumps over the lazy dog"
    index(expectedValue)
    suggest("the qui") should contain(expectedValue)
    suggest("the quick") should contain(expectedValue)
  }

  test("suggest returns relevant matches when the query is a prefix string of any token in an indexed field value") {
    val expectedValue = "Former President Abraham Lincoln"
    index(expectedValue)
    suggest("form") should contain(expectedValue)
    suggest("pres") should contain(expectedValue)
    suggest("abra") should contain(expectedValue)
    suggest("linc") should contain(expectedValue)
    suggest("former president abraham lincoln") should contain(expectedValue)
  }

  test("suggest does not tokenize an email address into its constituent parts") {
    val expectedValue = "we.are+awesome@socrata.com"
    index(expectedValue)
    //suggest("we.are+awesome@socrata.com") should contain(expectedValue)
    //suggest("we.are+awesome") should contain(expectedValue)
    // suggest("are") should be('empty)
    // suggest("awesome") should be('empty)
    // suggest("socrata") should be('empty)
    // suggest("com") should be('empty)    
  }

  test("suggest returns indexed field values that are URLs when the query is any of their consitituent terms") {
    val expectedValue = "https://lucene.rocks/completion-suggester.html"
    index(expectedValue)
    suggest("https") should contain(expectedValue)
    suggest("lucene") should contain(expectedValue)
    suggest("rocks") should contain(expectedValue)
    suggest("completion") should contain(expectedValue)
    suggest("suggester") should contain(expectedValue)
    suggest("html") should contain(expectedValue)
    suggest("https://lucene.rocks") should contain(expectedValue)
  }

  test("suggest returns indexed field values that contain non-english unicode when the query is non-english unicode") {
    val expectedValue = "愛" // scalastyle:ignore

    index(expectedValue)
    suggest(expectedValue) should contain(expectedValue)
  }

  test("suggest returns field values with currency symbols when currecny symbols are the query") {
    val expectedValue = "$500 and under"
    val search = "$"

    index(expectedValue)
    suggest(search) should contain(expectedValue)
  }

  // NOTE: using the edge ngram tokenizer does not allow for custom tokenization rules;
  // provided that we use an appropriate tokenizer at search time (ie. one that also
  // breaks on ampersands), then we will get the expected results, but in the case of
  // strings like 'AT&T' we'll potentially get a lot more than what we want
  test("suggest returns the expected field values when searching for terms with ampersands like AT&T") {
    val expectedValue = "AT&T Mobility"
    val search = "AT&T"

    index(expectedValue)
    suggest(search) should contain(expectedValue)
  }

  test("suggest returns the expected field values when the values in question contain slashes") {
    val expectedValue = "ANDREA H/ARTHUR D HARRIS"
    val search = "ANDREA H/ARTHUR"

    index(expectedValue)
    suggest(search) should contain(expectedValue)
  }

  test("suggest does not return anything when the query consists of a string that is tokenized away") {
    val value = "The quick and the dead"
    index(value)
    suggest(".") should be('empty)
  }

  test("suggest returns the expected field values when the values contain accented characters") {
    val value = "la mémoire d'éléphant"
    index(value)
    suggest("elephant") should contain(value)
  }
}
