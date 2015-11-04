package com.socrata.spandex.http

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.spandex.common.client.TestESClient
import com.socrata.spandex.common.{SpandexConfig, TestESData}
import com.socrata.spandex.http.SpandexResult.Fields._
import org.elasticsearch.common.unit.Fuzziness
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, ShouldMatchers}

class SpandexResultSpec extends FunSuiteLike with ShouldMatchers with TestESData with BeforeAndAfterAll {
  val config = new SpandexConfig
  val client = new TestESClient(config.es)

  override def beforeAll(): Unit = {
    removeBootstrapData()
    bootstrapData()
  }
  override def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  val opt0 = SpandexOption("0", None)
  val opt1 = SpandexOption("1", Some(1))
  val opt2 = SpandexOption("2", Some(2))
  val opt3 = SpandexOption("3", Some(3))
  val opt1String = JsonUtil.renderJson(opt1)
  val opt2String = JsonUtil.renderJson(opt2)
  val opt3String = JsonUtil.renderJson(opt3)

  test("json encode option") {
    val js = JsonUtil.renderJson(opt1)
    js should include(textJson)
    js should include(scoreJson)
  }

  test("json encode option with no score") {
    val js = JsonUtil.renderJson(opt0)
    js should include(textJson)
    js shouldNot include(scoreJson)
  }

  test("json encode result") {
    val js = JsonUtil.renderJson(SpandexResult(Seq(opt2, opt3)))
    js should include(optionsJson)
    js should include(opt2String)
    js should include(opt3String)
  }

  test("json encode empty result") {
    val js = JsonUtil.renderJson(SpandexResult(Seq.empty))
    js should include(optionsEmptyJson)
  }

  test("transform from suggest response") {
    val ds = datasets(0)
    val copy = copies(ds)(1)
    val col = columns(ds, copy)(2)
    val suggest = client.suggest(col, 10, "dat", Fuzziness.TWO, 3, 1)
    val js = JsonUtil.renderJson(SpandexResult(suggest))
    js should include(optionsJson)
    js should include(rows(col)(0).value)
  }

  test("transform from search response") {
    val ds = datasets(0)
    val copy = copies(ds)(1)
    val col = columns(ds, copy)(2)
    val sample = client.sample(col, 10)
    val js = JsonUtil.renderJson(SpandexResult(sample))
    js should include(optionsJson)
    js should include(rows(col)(0).value)
  }
}
