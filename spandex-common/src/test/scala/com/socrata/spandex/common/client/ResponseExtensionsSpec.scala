package com.socrata.spandex.common.client

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._
import com.socrata.datacoordinator.secondary.LifecycleStage
import org.elasticsearch.action.DocWriteRequest.OpType
import org.elasticsearch.action.DocWriteResponse.Result
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkResponse}
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.index.shard.ShardId
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.spandex.common.client.ResponseExtensions._

class ResponseExtensionsSpec extends FunSuiteLike
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  val shardId = ShardId.fromString(s"[spandex][0]")

  test("Create BulkResponseAcknowledgement from BulkResponse") {
    val itemResponses = Array(
      new BulkItemResponse(1, OpType.CREATE, new IndexResponse(shardId, "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(2, OpType.CREATE, new IndexResponse(shardId, "field_value", "alpha.1234", 0L, true)),
      new BulkItemResponse(3, OpType.CREATE, new IndexResponse(shardId, "column_map", "alpha.1234", 0L, true)),
      new BulkItemResponse(4, OpType.DELETE, new DeleteResponse(shardId, "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(5, OpType.UPDATE, new UpdateResponse(shardId, "field_value", "alpha.1234", 1L, Result.UPDATED)))

    val bulkResponse = new BulkResponse(itemResponses, 10L)
    val expectedAcknowledgement = BulkResponseAcknowledgement(
      deletions = Map("dataset_copy" -> 1),
      updates = Map("field_value" -> 1),
      creations = Map("dataset_copy" -> 1, "field_value" -> 1, "column_map" -> 1))

    BulkResponseAcknowledgement(bulkResponse) should be (expectedAcknowledgement)
  }

  test("Get non-empty deletions count map from BulkResponse") {
    val itemResponses = Array(
      new BulkItemResponse(1, OpType.CREATE, new IndexResponse(shardId, "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(2, OpType.CREATE, new IndexResponse(shardId, "field_value", "alpha.1234", 0L, true)),
      new BulkItemResponse(3, OpType.CREATE, new IndexResponse(shardId, "column_map", "alpha.1234", 0L, true)),
      new BulkItemResponse(4, OpType.DELETE, new DeleteResponse(shardId, "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(5, OpType.UPDATE, new UpdateResponse(shardId, "field_value", "alpha.1234", 1L, Result.UPDATED)))

    val bulkResponse = new BulkResponse(itemResponses, 10L)
    val expectedDeletionsCountMap = Map("dataset_copy" -> 1)

    bulkResponse.deletions should be (expectedDeletionsCountMap)
  }

  test("Get empty deletions count map from BulkResponse") {
    val itemResponses = Array(
      new BulkItemResponse(1, OpType.CREATE, new IndexResponse(shardId, "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(2, OpType.CREATE, new IndexResponse(shardId, "field_value", "alpha.1234", 0L, true)),
      new BulkItemResponse(3, OpType.CREATE, new IndexResponse(shardId, "column_map", "alpha.1234", 0L, true)),
      new BulkItemResponse(5, OpType.UPDATE, new UpdateResponse(shardId, "field_value", "alpha.1234", 1L, Result.UPDATED)))

    val bulkResponse = new BulkResponse(itemResponses, 10L)

    bulkResponse.deletions shouldBe empty
  }

  test("JSON serialzation of dataset copies, column maps, and column values includes composite fields") {
    val datasetId = "ds.one"
    val datasetCopy = DatasetCopy(datasetId, 1, 42, LifecycleStage.Published)
    var expected = j"""{"dataset_id":"ds.one","copy_number":1,"version":42,"stage":"Published"}"""
    JsonEncode.toJValue(datasetCopy) should be(expected)

    val columnMap = ColumnMap(datasetCopy.datasetId, datasetCopy.copyNumber, 2, "column2")
    expected = j"""{"dataset_id":"ds.one","copy_number":1,"system_column_id":2,"user_column_id":"column2"}"""
    JsonEncode.toJValue(columnMap) should be(expected)

    val columnValue = ColumnValue(columnMap.datasetId, columnMap.copyNumber, columnMap.systemColumnId, "foo", 1L)
    expected = j"""{"column_id":2,"composite_id":"ds.one|1|2","copy_number":1,"dataset_id":"ds.one","value":"foo","count": 1}"""
    JsonEncode.toJValue(columnValue) should be(expected)
  }
}
