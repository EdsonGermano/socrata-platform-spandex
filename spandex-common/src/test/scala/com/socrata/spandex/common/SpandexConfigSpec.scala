package com.socrata.spandex.common

import org.scalatest.{FunSuiteLike, Matchers}

class SpandexConfigSpec extends FunSuiteLike with Matchers {
  test("spandex config has these required values") {
    val conf = new SpandexConfig
    Some(conf.spandexPort) should be ('defined)
  }

  test("elasticsearch config has these required values") {
    val conf = new SpandexConfig().es
    Some(conf.host) should be ('defined)
    Some(conf.port) should be ('defined)
    Some(conf.clusterName) should be ('defined)
    Some(conf.index) should be ('defined)
    Some(conf.fieldValueMapping) should be ('defined)
    Some(conf.columnMapMapping) should be ('defined)
    Some(conf.datasetCopyMapping) should be ('defined)

  }
}
