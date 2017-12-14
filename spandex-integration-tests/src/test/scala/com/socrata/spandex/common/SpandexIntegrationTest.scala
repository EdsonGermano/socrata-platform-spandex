package com.socrata.spandex.common

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import com.socrata.spandex.common.client.SpandexESIntegrationTestClient

trait SpandexIntegrationTest extends BeforeAndAfterAll
  with BeforeAndAfterEach
  with TestData {

  this: Suite =>

  def indexName = getClass.getSimpleName.toLowerCase
  def client = SpandexESIntegrationTestClient("localhost", 9300, "es_dev", indexName, 10000, 60000)

  override protected def beforeAll(): Unit = {
    if (!client.isConnected) fail("Unable to connect to local Elasticsearch cluster")
    client.ensureIndex()
  }

  override def beforeEach(): Unit = {
    client.deleteAllDatasetCopies()
    client.bootstrapData(this)
  }

  override def afterEach(): Unit = client.removeBootstrapData(this)
  override def afterAll(): Unit = {
    client.deleteIndex()
    client.close()
  }  
}
