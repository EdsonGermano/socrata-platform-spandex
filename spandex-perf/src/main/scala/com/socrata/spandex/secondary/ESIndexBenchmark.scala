package com.socrata.spandex.secondary

import java.util.concurrent.TimeUnit

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client.{ColumnMap, FieldValue}
import com.socrata.spandex.common.{MarvelNames, PerfESClient}
import org.elasticsearch.action.index.IndexRequestBuilder
import org.openjdk.jmh.annotations._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 4)
@Measurement(iterations = 4)
@Threads(1)
@Fork(value = 4)
class ESIndexBenchmark extends MarvelNames {
  val client = new PerfESClient

  @Setup(Level.Trial)
  def setupIndex(): Unit = {
    client.bootstrapData()
  }

  @TearDown(Level.Trial)
  def teardownIndex(): Unit = {
    client.removeBootstrapData()
  }

  val datasetId = "optimus.42"
  var copyNum = 1L
  val columnSysId = 2L
  val columnUserId = "dead-beef"
  val version = 3L
  val stage = LifecycleStage.Published

  @Setup(Level.Iteration)
  def setupDataset(): Unit = {
    client.putDatasetCopy(datasetId, copyNum, version, stage, refresh = false)
    client.putColumnMap(ColumnMap(datasetId, copyNum, columnSysId, columnUserId), refresh = true)
    copyNum += 1
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

  private[this] def gcUltra(): Unit = {
    for {i <- 0 to 3} {
      Runtime.getRuntime.gc()
      Thread.sleep(55)
    }
  }

  @Benchmark
  def indexPhrase(): Unit = {
    @annotation.tailrec
    def go(n: Int, acc: List[IndexRequestBuilder]): List[IndexRequestBuilder] = n match {
      case 0 => acc
      case n: Int =>
        go(n - 1, client.fieldValueIndexRequest(FieldValue(datasetId, copyNum, columnSysId, n, anotherPhrase)) :: acc)
    }
    client.sendBulkRequest(go(10000, Nil), refresh = true)
  }

  @Benchmark
  def indexKeyword(): Unit = {
    @annotation.tailrec
    def go(n: Int, acc: List[IndexRequestBuilder]): List[IndexRequestBuilder] = n match {
      case 0 => acc
      case n: Int =>
        go(n - 1, client.fieldValueIndexRequest(FieldValue(datasetId, copyNum, columnSysId, n, anotherKeyword)) :: acc)
    }
    client.sendBulkRequest(go(10000, Nil), refresh = true)
  }
}
