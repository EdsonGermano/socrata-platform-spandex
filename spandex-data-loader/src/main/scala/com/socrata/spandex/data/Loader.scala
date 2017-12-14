package com.socrata.spandex.data

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.Map

import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import scopt.OptionParser
import com.socrata.datacoordinator.secondary.LifecycleStage
import org.elasticsearch.action.update.UpdateRequestBuilder
import com.socrata.spandex.common.client.{ColumnValue, ColumnMap, DatasetCopy, SpandexElasticSearchClient}

class Loader(val client: SpandexElasticSearchClient, val rowBatchSize: Int = Loader.RowBatchSize) {
  def loadFromPath(
    data: File,
    datasetCopy: DatasetCopy,
    columnMaps: Map[Int, ColumnMap]
  ): Unit = {
    val mapper = new CsvMapper()
    mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY)
    val schema = mapper.schemaFor(classOf[Array[String]]).withSkipFirstDataRow(true)
    val iterator = mapper.readerFor(classOf[Array[String]]).`with`(schema).readValues(data).asScala
    val DatasetCopy(datasetId, copyNumber, version, lifecycleStage) = datasetCopy

    client.putDatasetCopy(datasetId, copyNumber, version, lifecycleStage)
    columnMaps.values.foreach(columnMap => client.putColumnMap(columnMap))

    var rowsProcessed = 0

    iterator.grouped(rowBatchSize).foreach { rows: Seq[Array[String]] =>
      val columnValCountMap = mutable.Map[ColumnIdValue, ColumnValue]()

      rows.foreach { row =>
        val columnVals = row.zipWithIndex.collect {
          case (value, columnIndex) if columnMaps.contains(columnIndex) =>
            val columnMap = columnMaps(columnIndex)
            val colIdVal = ColumnIdValue(columnMap.systemColumnId, value)
            val defaultColumnVal = ColumnValue(datasetId, copyNumber, columnMap.systemColumnId, value, 0)
            val columnVal = columnValCountMap.getOrElse(colIdVal, defaultColumnVal)
            columnValCountMap += (colIdVal -> columnVal.copy(count = columnVal.count + 1))
        }

        rowsProcessed += 1
      }

      // scalastyle:ignore regex
      client.putColumnValues(datasetId, copyNumber, columnValCountMap.values.toList)

      if (rowsProcessed % 100000 == 0) {
        // scalastyle:ignore regex
        println(s"Processed $rowsProcessed rows")
      }
    }
  }
}

object Loader {
  val RowBatchSize = 10000

  private def parser = new OptionParser[LoaderConfig]("Autocomplete Data Loader") {
    head("Autocomplete Data Loader", BuildInfo.toJson)

    opt[File]("data-file").required().valueName("<file>").
      action((x, c) => c.copy(dataFile = x)).
      text("input-data is a path to file of data to load")

    opt[String]("dataset-id").required().valueName("<dataset-id>").
      action((x, c) => c.copy(datasetId = x)).
      text("the dataset identifer")

    opt[String]("include-column").unbounded().required().valueName("<column-index>,<column-name>").action {
      case (x, c) => c.copy(columns = c.columns :+ x)
    }.validate(x =>
      // scalastyle:ignore if.brace
      if (x.split(',').length == 2) success
      else failure("""Columns must be expressed as comma-separated index, name pairs (eg. "0,id" or "1,name")""")
    )
  }

  def main(args: Array[String]): Unit = {
  // NOTE: this will raise and thus exit if we are unable to parse the command-line args
    val LoaderConfig(dataFile: File, datasetId: String, columns: List[String]) =
      parser.parse(args, LoaderConfig()).getOrElse(LoaderConfig())

    // scalastyle:ignore magic.number
    val esClient = new SpandexElasticSearchClient("localhost", 9300, "es_dev", "spandex", 10000, 60000, 64)

    val copyNumber = 1L
    val version = 1L
    val datasetCopy = DatasetCopy(datasetId, copyNumber, version, LifecycleStage.Published)

    val columnMaps = columns.flatMap(column =>
      column.split(',') match {
        case Array(colIdx, colName) =>
          Some((colIdx.toInt, ColumnMap(datasetId, copyNumber, colIdx.toLong, colName)))
        case _ => None
      }
    ).toMap

    val loader = new Loader(esClient)
    loader.loadFromPath(dataFile, datasetCopy, columnMaps)
  }
}
