import javax.servlet.ServletContext

import com.socrata.spandex.common.SpandexConfig
import com.socrata.spandex.common.client.SpandexElasticSearchClient
import com.socrata.spandex.http.SpandexResult.Fields._
import com.socrata.spandex.http.SpandexServlet
import org.scalatra._
import org.scalatra.metrics.MetricsSupportExtensions._
import org.scalatra.metrics._

class ScalatraBootstrap extends LifeCycle with MetricsBootstrap {
  val conf = new SpandexConfig
  val client = new SpandexElasticSearchClient(conf.es)

  override def init(context: ServletContext): Unit = {
    context.mountHealthCheckServlet("/health")
    context.mountThreadDumpServlet("/thread-dump")
    context.installInstrumentedFilter(s"/$routeSample/*")
    context.installInstrumentedFilter(s"/$routeSuggest/*")
    context.mount(new SpandexServlet(conf, client), "/*")
  }

  override def destroy(context: ServletContext): Unit = {
    client.close()
  }
}
