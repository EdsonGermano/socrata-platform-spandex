package com.socrata.spandex.http

import com.socrata.spandex.common._
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

object Spandex extends App {
  val conf = new SpandexConfig

  SpandexBootstrap.ensureIndex(conf)

  val port = conf.port
  val pathRoot = "/"

  val context = new WebAppContext
  context.setContextPath(pathRoot)
  context.setResourceBase("src/main/webapp")
  context.addEventListener(new ScalatraListener)
  context.addServlet(classOf[DefaultServlet], pathRoot)

  val server = new Server(port)
  server.setHandler(context)
  server.start()
  server.join()
}
