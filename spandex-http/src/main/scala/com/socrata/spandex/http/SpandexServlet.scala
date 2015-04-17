package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.spandex.common._
import com.socrata.spandex.common.client._
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.common.unit.Fuzziness

import scala.util.Try

class SpandexServlet(conf: SpandexConfig,
                     client: => SpandexElasticSearchClient) extends SpandexStack {
  def index: String = conf.es.index

  val version = JsonUtil.renderJson(JObject(BuildInfo.toMap.mapValues(v => JString(v.toString))))

  def columnMap(datasetId: String, copyNum: Long, userColumnId: String): ColumnMap =
    client.columnMap(datasetId, copyNum, userColumnId).getOrElse(halt(
      HttpStatus.SC_NOT_FOUND, JsonUtil.renderJson(SpandexError("Column not found", Some(userColumnId)))))
  def urlDecode(s: String): String = java.net.URLDecoder.decode(s, EncodingUtf8)

  healthCheck("alive") {true}
  healthCheck("version") {Try {version}}
  healthCheck("esClusterHealth") {Try {esClusterHealth}}

  get("/version") {
    logger.info(">>> /version")
    contentType = ContentTypeJson
    logger.info(s"<<< $version")
    version
  }

  get("/") {
    // TODO: getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  def esClusterHealth: String = {
    val clusterAdminClient = client.client.admin().cluster()
    val req = new ClusterHealthRequest(index)
    clusterAdminClient.health(req).actionGet().toString
  }

  private[this] val routeSuggest = "suggest"
  private[this] val paramDatasetId = "datasetId"
  private[this] val paramCopyNum = "copyNum"
  private[this] val paramUserColumnId = "userColumnId"
  private[this] val paramText = "text"
  get(s"/$routeSuggest/:$paramDatasetId/:$paramCopyNum/:$paramUserColumnId/:$paramText") {
    timer("suggestText") {
      suggest { (col, text, fuzz, size) =>
        SpandexResult(client.suggest(col, size, text, fuzz, conf.suggestFuzzLength, conf.suggestFuzzPrefix))
      }
    }.call()
  }

  /* How to get all the results out of Lucene.
   * Ignore the provided text and fuzziness parameters and replace as follows.
   * Text "1 character" => blank string is not allowed, but through the fuzziness below
   *                       this 1 character will be factored out.
   * Fuzziness ONE => approximately allows 1 edit distance from the given to result texts.
   * Fuzz Length 0 => start giving fuzzy results from any length of input text.
   * Fuzz Prefix 0 => allow all results no matter how badly matched.
   * TA-DA!
   */
  private[this] val sampleText = "a"
  private[this] val sampleFuzz = Fuzziness.ONE
  private[this] val sampleFuzzLen = 0
  private[this] val sampleFuzzPre = 0
  get(s"/$routeSuggest/:$paramDatasetId/:$paramCopyNum/:$paramUserColumnId") {
    timer("suggestSample") {
      suggest { (col, _, _, size) =>
        SpandexResult(client.suggest(col, size, sampleText, sampleFuzz, sampleFuzzLen, sampleFuzzPre))
      }
    }.call()
  }

  def suggest(f: (ColumnMap, String, Fuzziness, Int) => SpandexResult): String = {
    val copyNumMustBeNumeric = "Copy number must be numeric"
    val paramFuzz = "fuzz"
    val paramSize = "size"

    contentType = ContentTypeJson
    val datasetId = params.get(paramDatasetId).get
    val copyNumText = params.get(paramCopyNum).get
    val copyNum = Try(copyNumText.toLong).getOrElse(halt(
      HttpStatus.SC_BAD_REQUEST, JsonUtil.renderJson(SpandexError(copyNumMustBeNumeric, Some(copyNumText)))))
    val userColumnId = params.get(paramUserColumnId).get
    val text = urlDecode(params.get(paramText).getOrElse(""))
    val fuzz = Fuzziness.build(params.getOrElse(paramFuzz, conf.suggestFuzziness))
    val size = params.get(paramSize).headOption.fold(conf.suggestSize)(_.toInt)
    logger.info(s">>> $datasetId, $copyNum, $userColumnId, $text, ${fuzz.asDistance}, $size")

    val column = columnMap(datasetId, copyNum, userColumnId)
    logger.info(s"found column $column")

    val result = f(column, text, fuzz, size)
    logger.info(s"<<< $result")
    JsonUtil.renderJson(result)
  }

  /* Not yet used.
   * sample endpoint exposes query by column with aggregation on doc count
   */
  get(s"/sample/:$paramDatasetId/:$paramCopyNum/:$paramUserColumnId") {
    timer("sample") {
      suggest { (col, _, _, size) => SpandexResult(client.sample(col, size)) }
    }.call()
  }

}
