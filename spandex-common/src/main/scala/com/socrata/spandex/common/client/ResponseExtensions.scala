package com.socrata.spandex.common.client

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, JsonUtil, Strategy}
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.secondary.{ColumnInfo, LifecycleStage}
import com.socrata.soql.types.SoQLText
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.Terms

import scala.collection.JavaConverters._
import scala.language.implicitConversions

@JsonKeyStrategy(Strategy.Underscore)
case class DatasetCopy(datasetId: String, copyNumber: Long, version: Long, stage: LifecycleStage) {
  lazy val docId = DatasetCopy.makeDocId(datasetId, copyNumber)
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
case class ColumnMap(datasetId: String,
                     copyNumber: Long,
                     systemColumnId: Long,
                     userColumnId: String) {
  lazy val docId = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
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
}

@JsonKeyStrategy(Strategy.Underscore)
case class FieldValue(datasetId: String,
                      copyNumber: Long,
                      columnId: Long,
                      rowId: Long,
                      value: String) {
  lazy val docId = FieldValue.makeDocId(datasetId, copyNumber, columnId, rowId)
}
object FieldValue {
  implicit val jCodec = AutomaticJsonCodecBuilder[FieldValue]

  def apply(datasetName: String, copyNumber: Long, columnId: ColumnId, rowId: RowId, data: SoQLText): FieldValue =
    this(datasetName, copyNumber, columnId.underlying, rowId.underlying, data.value)

  def makeDocId(datasetId: String, copyNumber: Long, columnId: Long, rowId: Long): String =
    s"$datasetId|$copyNumber|$columnId|$rowId"
}

@JsonKeyStrategy(Strategy.Underscore)
case class BucketCount(key: String, docCount: Long)
object BucketCount {
  implicit val jCodec = AutomaticJsonCodecBuilder[BucketCount]
}

case class SearchResults[T: JsonDecode](totalHits: Long, thisPage: Seq[T], aggs: Seq[BucketCount])

object ResponseExtensions {
  implicit def toExtendedResponse(response: SearchResponse): SearchResponseExtensions =
    SearchResponseExtensions(response)

  implicit def toExtendedResponse(response: GetResponse): GetResponseExtensions =
    GetResponseExtensions(response)
}

case class SearchResponseExtensions(response: SearchResponse) {
  def results[T : JsonDecode]: SearchResults[T] = results(None)
  def results[T : JsonDecode](aggKey: String): SearchResults[T] = results(Some(aggKey))
  protected def results[T : JsonDecode](aggKey: Option[String]): SearchResults[T] = {
    val hits = Option(response.getHits).fold(Seq.empty[SearchHit])(_.getHits.toSeq)
    val sources = hits.map { hit => Option(hit.getSourceAsString) }.flatten
    val thisPage = sources.map { source => JsonUtil.parseJson[T](source).right.get }
    val totalHits = Option(response.getHits).fold(0L)(_.totalHits)

    /* Not yet used.
     * captures search aggregation results
     * TODO: multiple aggs at once
     */
    val aggs = aggKey.fold(Seq.empty[BucketCount]) { k =>
      response.getAggregations.get[Terms](k)
        .getBuckets.asScala.map { b => BucketCount(b.getKey, b.getDocCount) }
        .toSeq
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
