package com.socrata.spandex.http

import com.socrata.spandex.common._
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

import scala.util.Try

object Spandex extends App {
  lazy val conf = new SpandexConfig
  var ready: Boolean = false

  // TODO : Determine how we want index deployment/sanity check
  //        to actually work on each Spandex environment.
  //        In this current model, we silently create entire indexes,
  //        or silently fail to.
  //        We could continue to go this route, or we could follow the
  //        Core/NBE model where any changes to the underlying
  //        store have to be manually executed via a migration.
  // SpandexBootstrap.ensureIndex(conf, conf.es.port)
  val port = conf.spandexPort
  val pathRoot = "/"

  val context = new WebAppContext
  context.setContextPath(pathRoot)
  context.setResourceBase("src/main/webapp")
  context.addEventListener(new ScalatraListener)
  context.addServlet(classOf[DefaultServlet], pathRoot)

  val server = new Server(port)
  server.setHandler(context)
  Try {
    server.start()
    ready = true
    server.join()
  }
  server.stop()
}
