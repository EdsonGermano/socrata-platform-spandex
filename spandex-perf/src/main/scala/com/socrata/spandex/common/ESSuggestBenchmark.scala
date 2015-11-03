package com.socrata.spandex.common

import java.util.concurrent.TimeUnit

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client.{ColumnMap, FieldValue}
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.common.unit.Fuzziness
import org.openjdk.jmh.annotations._

import scala.util.Random

// scalastyle:off magic.number
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 8)
@Measurement(iterations = 4)
@Threads(1)
@Fork(value = 4)
class ESSuggestBenchmark extends MarvelNames {
  val client = new PerfESClient
  val maxInput = 32

  @Setup(Level.Trial)
  def setupIndex(): Unit = {
    client.bootstrapData()
  }

  @TearDown(Level.Trial)
  def teardownIndex(): Unit = {
    client.removeBootstrapData()
  }

  val datasetId = "optimus.42"
  var copyNumber = 1L
  val systemColumnId = 2L
  val userColumndId = "dead-beef"
  val version = 3L
  val stage = LifecycleStage.Published
  val col = ColumnMap(datasetId, copyNumber, systemColumnId, userColumndId)
  @Setup(Level.Iteration)
  def setupDataset(): Unit = {
    client.putDatasetCopy(datasetId, copyNumber, version, stage, refresh = false)
    client.putColumnMap(ColumnMap(datasetId, copyNumber, systemColumnId, userColumndId), refresh = false)
    copyNumber += 1
    index()
    gcUltra()
  }

  @TearDown(Level.Iteration)
  def teardownDataset(): Unit = {
    client.deleteFieldValuesByDataset(datasetId)
    client.deleteColumnMapsByDataset(datasetId)
    client.deleteDatasetCopiesByDataset(datasetId)
    client.refresh()
    Thread.sleep(1000L) // make sure the index is clear before next iteration
  }

  def index(): Unit = {
    @annotation.tailrec
    def go(n: Int, acc: List[IndexRequestBuilder]): List[IndexRequestBuilder] = n match {
      case 0 => acc
      case n: Int =>
        val fv = FieldValue(datasetId, copyNumber, systemColumnId, n, anotherPhrase)
        go(n - 1, client.fieldValueIndexRequest(fv) :: acc)
    }
    client.sendBulkRequest(go(10000, Nil), refresh = true)
  }

  private[this] def gcUltra(): Unit = {
    for {i <- 0 to 3} {
      Runtime.getRuntime.gc()
      Thread.sleep(55)
    }
  }

  @Benchmark
  def suggest(): Unit = {
    @annotation.tailrec
    def go(n: Int): Unit = {
      if (n > 0) {
        val r = Random.nextInt(maxInput)
        val s = anotherPhrase
        val sub = s.substring(0, Math.min(r, s.length))
        client.suggest(col, 10, sub, Fuzziness.ZERO, 0, 0)
        go(n - 1)
      }
    }
    go(10000)
  }
}
