package com.socrata.spandex.http

import org.scalatest.{MustMatchers, FunSuiteLike}

class SpandexSpec extends FunSuiteLike with MustMatchers {
  test("spawn") {
    val thread = new Thread {
      override def run(): Unit = {
        // TODO: fix: this exception test requires manual observation
        a[InterruptedException] should be thrownBy {
          Spandex.main(Array.empty)
        }
      }
    }
    thread.start()
    while (!Spandex.ready) {
      Thread.sleep(1000)
    }
    thread.interrupt()
  }
}
