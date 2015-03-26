package com.socrata.spandex

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import wabisabi.{Client => ElasticsearchClient}

import scala.concurrent.Await

object SpandexBootstrap {
  def ensureIndex(conf: SpandexConfig): String = {
    val esc = new ElasticsearchClient(conf.esUrl)
    val indexResponse = Await.result(esc.verifyIndex(conf.index), conf.escTimeoutFast)
    val resultHttpCode = indexResponse.getStatusCode
    if (resultHttpCode != HttpStatus.SC_OK) {
      Await.result(esc.createIndex(conf.index, Some(conf.indexSettings)), conf.escTimeoutFast).getResponseBody
    } else {
      indexResponse.getResponseBody
    }
  }
}