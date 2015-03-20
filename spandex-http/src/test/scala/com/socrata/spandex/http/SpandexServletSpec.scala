package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.socrata.spandex.common._
import org.scalatest.FunSuiteLike
import org.scalatra.test.scalatest._
import wabisabi.{Client => ElasticsearchClient}

import scala.concurrent.Await

// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class SpandexServletSpec extends ScalatraSuite with FunSuiteLike {
  val conf = new SpandexConfig
  val localMasterPort = 9211
  val esMaster = new ElasticsearchServer(localMasterPort, true)
  val fxf = "dead-beef"
  val copy = "1"
  val fxfCopy = s"$fxf-$copy"

  override def beforeAll(): Unit = {
    super.beforeAll()
    esMaster.start()
    SpandexBootstrap.ensureIndex(conf, localMasterPort)
    imagineSomeMockData()
    addServlet(new SpandexServlet(conf, localMasterPort), "/*")
  }

  override def afterAll(): Unit = {
    esMaster.stop()
    super.afterAll()
  }

  def imagineSomeMockData(): Unit = {
    val esc = new ElasticsearchClient(conf.esUrl(localMasterPort))

    Await.result(
      esc.index(conf.es.index, fxf, Some(copy), "{\"truthVersion\":\"0\", \"truthUpdate\":\"1234567890\"}"),
      conf.escTimeoutFast)

    val newMapping =
      """{
        |  "%s":{
        |    "_all":{"enabled":false},
        |    "properties":{
        |      "crime":{
        |        "type":"completion",
        |        "analyzer":"simple",
        |        "payloads":false,
        |        "preserve_separators":false,
        |        "preserve_position_increments":false,
        |        "max_input_length":50
        |      },
        |      "crimeType":{
        |        "type":"completion",
        |        "analyzer":"simple",
        |        "payloads":false,
        |        "preserve_separators":false,
        |        "preserve_position_increments":false,
        |        "max_input_length":50
        |      }
        |    }
        |  }
        |}
      """.stripMargin.format(fxfCopy)
    val rMap = Await.result(esc.putMapping(Seq(conf.es.index), fxfCopy, newMapping), conf.escTimeoutFast)

    val newBulkData =
      """{"index": {"_id": "1"} }
        |{"crimeType": "NARCOTICS"}
        |{"index": {"_id": "2"} }
        |{"crimeType": "PUBLIC INDECENCY"}
      """.stripMargin
    val rBulk = Await.result(esc.bulk(Some(conf.es.index), Some(fxfCopy), newBulkData), conf.escTimeoutFast)

    // wait a sec for elasticsearch to process the documents into lucene
    Thread.sleep(1000) // scalastyle:ignore magic.number
  }

  test("get of index page") {
    get("/") {
      status should equal (HttpStatus.SC_OK)
      val contentType: String = header.getOrElse("Content-Type", "")
      contentType should include ("text/html")
      body should include ("Hello, spandex")
    }
  }

  test("get of non-existent page") {
    get("/goodbye-world") {
      status should equal (HttpStatus.SC_NOT_FOUND)
    }
  }

  test("get health status page") {
    get("/health") {
      status should equal (HttpStatus.SC_OK)
      body should include ("\"status\"")
    }
  }

  test("get all spandex mappings") {
    get("/mapping") {
      status should equal (HttpStatus.SC_OK)
      body should include ("mappings")
    }
  }

  test("suggest") {
    get(s"/suggest/$fxf/crimeType/nar") {
      status should equal (HttpStatus.SC_OK)
      body should include ("NARCOTICS")
      body shouldNot include ("PUBLIC INDECENCY")
    }
  }

  test("suggest without required params should return 404") {
    get(s"/suggest/$fxf/crimeType/") {
      status should equal (HttpStatus.SC_NOT_FOUND)
    }
    get(s"/suggest/$fxf/") {
      status should equal (HttpStatus.SC_NOT_FOUND)
    }
    get("/suggest/") {
      status should equal (HttpStatus.SC_NOT_FOUND)
    }
  }
}
