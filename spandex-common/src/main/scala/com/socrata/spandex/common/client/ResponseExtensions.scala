package com.socrata.spandex.common.client

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

import com.rojoma.json.v3.ast.{JObject, JNumber, JString, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AutomaticJsonDecodeBuilder, JsonKeyStrategy, JsonUtil, Strategy} // scalastyle:ignore line.size.limit

import com.socrata.datacoordinator.secondary.{ColumnInfo, LifecycleStage}
import org.elasticsearch.action.DocWriteRequest.OpType
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.Aggregations
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.max.Max


@JsonKeyStrategy(Strategy.Underscore)
case class DatasetCopy(datasetId: String, copyNumber: Long, version: Long, stage: LifecycleStage) {
  val docId = DatasetCopy.makeDocId(datasetId, copyNumber)
}

object DatasetCopy {
  implicit val lifecycleStageCodec = new JsonDecode[LifecycleStage] with JsonEncode[LifecycleStage] {
    def decode(x: JValue): JsonDecode.DecodeResult[LifecycleStage] = {
      x match {
        case JString(stage) =>
          // Account for case differences
          LifecycleStage.values.find(_.toString.toLowerCase == stage.toLowerCase) match {
            case Some(matching) => Right(matching)
            case None           => Left(DecodeError.InvalidValue(x))
          }
        case _              => Left(DecodeError.InvalidType(JString, x.jsonType))
      }
    }

    def encode(x: LifecycleStage): JValue = JString(x.toString)
  }
  implicit val jCodec = AutomaticJsonCodecBuilder[DatasetCopy]

  def makeDocId(datasetId: String, copyNumber: Long): String = s"$datasetId|$copyNumber"
}

@JsonKeyStrategy(Strategy.Underscore)
case class ColumnMap(
    datasetId: String,
    copyNumber: Long,
    systemColumnId: Long,
    userColumnId: String) {
  val docId = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
  val compositeId = ColumnMap.makeCompositeId(datasetId, copyNumber, systemColumnId)
}

object ColumnMap {
  implicit val jCodec = AutomaticJsonCodecBuilder[ColumnMap]

  def apply(datasetId: String,
            copyNumber: Long,
            columnInfo: ColumnInfo[_]): ColumnMap =
    this(datasetId,
         copyNumber,
         columnInfo.systemId.underlying,
         columnInfo.id.underlying)

  def makeDocId(datasetId: String,
                copyNumber: Long,
                userColumnId: String): String =
    s"$datasetId|$copyNumber|$userColumnId"

  def makeCompositeId(datasetId: String, copyNumber: Long, systemColumnId: Long): String =
    s"$datasetId|$copyNumber|$systemColumnId"
}

@JsonKeyStrategy(Strategy.Underscore)
case class CompositeId(compositeId: String)

object CompositeId {
  implicit val codec = AutomaticJsonCodecBuilder[CompositeId]
}

@JsonKeyStrategy(Strategy.Underscore)
case class SuggestWithContext(input: Seq[String], contexts: CompositeId)

object SuggestWithContext {
  implicit val codec = AutomaticJsonCodecBuilder[SuggestWithContext]
}

@JsonKeyStrategy(Strategy.Underscore)
case class FieldValue(
    datasetId: String,
    copyNumber: Long,
    columnId: Long,
    rowId: Long,
    value: String) {
  val docId = FieldValue.makeDocId(datasetId, copyNumber, columnId, rowId)
  val compositeId = FieldValue.makeCompositeId(datasetId, copyNumber, columnId)

  def isNonEmpty: Boolean = value != null && value.trim.nonEmpty  // scalastyle:ignore null
}

object FieldValue {
  implicit object encode extends JsonEncode[FieldValue] { // scalastyle:ignore object.name
    def encode(fieldValue: FieldValue): JValue =
      JObject(
        Map(
          "column_id" -> JNumber(fieldValue.columnId),
          "composite_id" -> JString(fieldValue.compositeId),
          "copy_number" -> JNumber(fieldValue.copyNumber),
          "dataset_id" -> JString(fieldValue.datasetId),
          "row_id" -> JNumber(fieldValue.rowId),
          "value" -> JString(fieldValue.value)
        ))
  }

  implicit val decode = AutomaticJsonDecodeBuilder[FieldValue]

  def makeDocId(datasetId: String, copyNumber: Long, columnId: Long, rowId: Long): String =
    s"$datasetId|$copyNumber|$columnId|$rowId"

  def makeCompositeId(datasetId: String, copyNumber: Long, columnId: Long): String =
    s"$datasetId|$copyNumber|$columnId"
}

@JsonKeyStrategy(Strategy.Underscore)
case class BucketKeyVal(key: String, value: Double)

object BucketCount {
  implicit val jCodec = AutomaticJsonCodecBuilder[BucketKeyVal]
}

case class SearchResults[T: JsonDecode](totalHits: Long, thisPage: Seq[T], aggs: Seq[BucketKeyVal])

object ResponseExtensions {
  implicit def toExtendedResponse(response: SearchResponse): SearchResponseExtensions =
    SearchResponseExtensions(response)

  implicit def toExtendedResponse(response: GetResponse): GetResponseExtensions =
    GetResponseExtensions(response)

  implicit def toExtendedResponse(response: BulkResponse): BulkResponseExtensions =
    BulkResponseExtensions(response)
}

case class SearchResponseExtensions(response: SearchResponse) {
  def results[T : JsonDecode]: SearchResults[T] = results(None)

  def results[T : JsonDecode](aggKey: String): SearchResults[T] = results(Some(aggKey))

  private def bucketDocCountOrScore(bucket: Terms.Bucket): Double = {
    val aggs = bucket.getAggregations
    aggs.get[Max]("max_score") match {
      case null => bucket.getDocCount.toDouble // scalastyle:ignore null
      case maxScore: Max => maxScore.getValue
    }
  }

  protected def results[T : JsonDecode](aggKey: Option[String]): SearchResults[T] = {
    val hits = Option(response.getHits).fold(Seq.empty[SearchHit])(_.getHits.toSeq)
    val sources = hits.map { hit => Option(hit.getSourceAsString) }.flatten
    val thisPage = sources.map { source => JsonUtil.parseJson[T](source).right.get }
    val totalHits = Option(response.getHits).fold(0L)(_.totalHits)

    val aggs = aggKey.fold(Seq.empty[BucketKeyVal]) { k =>
      // added toString. but that's not right.
      response.getAggregations.get[Terms](k)
        .getBuckets.asScala.map { b =>
          BucketKeyVal(b.getKey.toString, bucketDocCountOrScore(b))
        }.toSeq
    }

    SearchResults(totalHits, thisPage, aggs)
  }
}

case class GetResponseExtensions(response: GetResponse) {
  def result[T : JsonDecode]: Option[T] = {
    val source = Option(response.getSourceAsString)
    source.map { s => JsonUtil.parseJson[T](s).right.get }
  }
}

case class BulkResponseAcknowledgement(
    deletions: Map[String, Int],
    updates: Map[String, Int],
    creations: Map[String, Int])

object BulkResponseAcknowledgement {
  def empty: BulkResponseAcknowledgement = BulkResponseAcknowledgement(Map.empty, Map.empty, Map.empty)

  def apply(bulkResponse: BulkResponse): BulkResponseAcknowledgement = {
    val deletions = mutable.Map[String, Int]()
    val updates = mutable.Map[String, Int]()
    val creations = mutable.Map[String, Int]()

    bulkResponse.getItems.toList.foreach { itemResponse =>
      val countsToUpdate = itemResponse.getOpType match {
        case OpType.DELETE => deletions
        case OpType.UPDATE => updates
        case OpType.CREATE => creations
        case _ => mutable.Map.empty[String, Int]
      }

      countsToUpdate += (itemResponse.getType -> (countsToUpdate.getOrElse(itemResponse.getType, 0) + 1))
    }

    BulkResponseAcknowledgement(deletions.toMap, updates.toMap, creations.toMap)
  }
}

case class BulkResponseExtensions(response: BulkResponse) {
  def deletions: Map[String, Int] = BulkResponseAcknowledgement(response).deletions
}
