package com.socrata.spandex.common

import scala.collection.JavaConverters._

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.spandex.common.client._

case class Token(text: String, offset: Int)

object Token {
  def apply(token: AnalyzeToken): Token =
    Token(token.getTerm, token.getStartOffset)
}

class AnalyzersSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll {
  val testSuiteName = getClass.getSimpleName.toLowerCase
  val client = new TestESClient(testSuiteName)

  override def beforeAll(): Unit = {
    SpandexElasticSearchClient.ensureIndex(getClass.getSimpleName.toLowerCase, client)
  }

  override def afterAll(): Unit = {
    client.deleteIndex()
    client.close()
  }

  def analyze(analyzer: String, text: String): List[Token] = {
    val request = new AnalyzeRequest(testSuiteName).analyzer(analyzer).text(text)
    client.client.admin().indices().analyze(request).actionGet().getTokens.asScala.map(Token.apply).toList
  }

  "the autocomplete analyzer should preserve tokens with accented characters" in {
    analyze("autocomplete", "systématique") should contain allOf (Token("systématique", 0), Token("systematique", 0))
  }

  "the case_insensitive_word query analyzer should not normalize accented characters" in {
    analyze("case_insensitive_word", "systématique") should contain (Token("systématique", 0))
  }
}
