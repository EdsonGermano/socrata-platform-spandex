package com.socrata.spandex.common

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client._

class SuggestionsSpec extends FunSuiteLike with Matchers with BeforeAndAfterAll {
  val className = getClass.getSimpleName.toLowerCase

  protected lazy val client = new TestESClient(getClass.getSimpleName.toLowerCase)

  override def beforeAll(): Unit = {
    SpandexElasticSearchClient.ensureIndex(getClass.getSimpleName.toLowerCase, client)
  }

  override def afterAll(): Unit = {
    client.deleteIndex()
    client.close()
  }

  val ds = "ds.one"
  val copy = DatasetCopy(ds, 1, 42, LifecycleStage.Published)
  val col = ColumnMap(copy.datasetId, copy.copyNumber, 2, "column2")

  def index(value: String, count: Long = 1): Unit =
    client.indexColumnValues(
      List(ColumnValue(col.datasetId, col.copyNumber, col.systemColumnId, value, count)),
      refresh = true
    )

  def suggest(query: String): List[String] = {
    val response = client.suggest(col, 20, query)
    response.thisPage.map {
      case ScoredResult(ColumnValue(_, _, _, value, _), _) => value
    }.toList
  }

  test("suggest is case insensitive and supports numbers and symbols") {
    val fool = ColumnValue(col.datasetId, col.copyNumber, col.systemColumnId, "fool", 1)
    val food = ColumnValue(col.datasetId, col.copyNumber, col.systemColumnId, "FOOD", 1)
    val date = ColumnValue(col.datasetId, col.copyNumber, col.systemColumnId, "04/2014", 1)
    val sym  = ColumnValue(col.datasetId, col.copyNumber, col.systemColumnId, "@giraffe", 1)

    client.indexColumnValues(List(fool, food, date, sym), refresh = true)

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

  test("suggest returns values ordered by count when the suggest query is an empty string") {
    val foo = ColumnValue(col.datasetId, col.copyNumber, col.systemColumnId, "foo", 1)
    val bar = ColumnValue(col.datasetId, col.copyNumber, col.systemColumnId, "bar", 2)
    val baz = ColumnValue(col.datasetId, col.copyNumber, col.systemColumnId, "baz", 3)
    client.indexColumnValues(List(foo, bar, baz), refresh = true)

    suggest("") should contain inOrder ("baz", "bar", "foo")
  }
}
