package com.socrata.spandex.common

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 8)
@Measurement(iterations = 4)
@Threads(1)
@Fork(value = 4)
class CompletionAnalyzerBenchmark extends MarvelNames {
  @Benchmark
  def analyzeCompletionTokens(): Unit = {
    @annotation.tailrec
    def go(n: Int): Unit = {
      if (n > 0) {
        CompletionAnalyzer.analyze(anotherPhrase)
        go(n - 1)
      }
    }
    go(10000)
  }
}
