package com.socrata.spandex.common.client

import com.socrata.datacoordinator.secondary.LifecycleStage
import org.elasticsearch.action.index.IndexRequestBuilder
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.spandex.common.client.ResponseExtensions._
import com.socrata.spandex.common.client.SpandexElasticSearchClient._
import com.socrata.spandex.common.TestESData

// scalastyle:off
class SpandexElasticSearchClientSpec extends FunSuiteLike
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with TestESData {

  val indexName = getClass.getSimpleName.toLowerCase
  val client = new TestESClient(indexName)

  override def afterAll(): Unit = client.close()

  override def beforeEach(): Unit = {
    client.deleteAllDatasetCopies()
    bootstrapData()
  }

  override def afterEach(): Unit = removeBootstrapData()

  def verifyColumnValue(columnValue: ColumnValue): Option[ColumnValue] =
    client.client
      .prepareGet(indexName, ColumnValueType, columnValue.docId)
      .execute.actionGet
      .result[ColumnValue]

  test("Insert, update, get column values, then delete and get again") {
    val toInsert = Seq(
      ColumnValue("alpha.1337", 1, 20, "axolotl", 1L),
      ColumnValue("alpha.1337", 1, 21, "amphibious", 1L),
      ColumnValue("alpha.1337", 1, 22, "Henry", 1L))

    toInsert.foreach { fv =>
      verifyColumnValue(fv) should not be 'defined
    }

    val inserts = toInsert.map(client.columnValueIndexRequest)
    client.sendBulkRequest(inserts, refresh = true)

    toInsert.foreach { fv =>
      verifyColumnValue(fv) should be (Some(fv))
    }

    val toUpdate = Seq(
      ColumnValue("alpha.1337", 1, 20, "axolotl", 1L),
      ColumnValue("alpha.1337", 1, 22, "Henry", 1L))

    val updates = toUpdate.map(client.columnValueIndexRequest)
    client.sendBulkRequest(updates, refresh = true)

    verifyColumnValue(toInsert(0)).get should be (toUpdate(0))
    verifyColumnValue(toInsert(1)).get should be (toInsert(1))
    verifyColumnValue(toInsert(2)).get should be (toUpdate(1))

    val deletes = toUpdate.map(columnValue =>
      client.deleteColumnValuesByColumnId(
        columnValue.datasetId, columnValue.copyNumber, columnValue.columnId, refresh = true))

    verifyColumnValue(toInsert(0)) should not be 'defined
    verifyColumnValue(toInsert(1)).get should be (toInsert(1))
    verifyColumnValue(toInsert(2)) should not be 'defined
  }

  test("Don't send empty bulk requests to Elasticsearch") {
    val empty = Seq.empty[IndexRequestBuilder]
    // BulkRequest.validate throws if given an empty bulk request
    client.sendBulkRequest(empty, refresh = true)
  }

  test("Delete field values by dataset") {
    client.searchColumnValuesByDataset(datasets(0)).totalHits should be (45)
    client.searchColumnValuesByDataset(datasets(1)).totalHits should be (45)

    client.deleteColumnValuesByDataset(datasets(0), refresh=true)

    client.searchColumnValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnValuesByDataset(datasets(1)).totalHits should be (45)
  }

  test("Delete field values by copy number") {
    client.searchColumnValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(0), 2).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 2).totalHits should be (15)

    client.deleteColumnValuesByCopyNumber(datasets(0), 2, refresh=true)

    client.searchColumnValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
    client.searchColumnValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
  }

  test("Delete field values by column") {
    client.searchColumnValuesByColumnId(datasets(0), 1, 1).totalHits should be (5)
    client.searchColumnValuesByColumnId(datasets(0), 2, 1).totalHits should be (5)
    client.searchColumnValuesByColumnId(datasets(0), 2, 2).totalHits should be (5)
    client.searchColumnValuesByColumnId(datasets(1), 2, 1).totalHits should be (5)

    client.deleteColumnValuesByColumnId(datasets(0), 2, 1, refresh=true)

    client.searchColumnValuesByColumnId(datasets(0), 1, 1).totalHits should be (5)
    client.searchColumnValuesByColumnId(datasets(0), 2, 1).totalHits should be (0)
    client.searchColumnValuesByColumnId(datasets(0), 2, 2).totalHits should be (5)
    client.searchColumnValuesByColumnId(datasets(1), 2, 1).totalHits should be (5)
  }

  // test("Delete field values by row") {
  //   client.searchColumnValuesByRowId(datasets(0), 2, 1).totalHits should be (3)
  //   client.searchColumnValuesByRowId(datasets(0), 2, 2).totalHits should be (3)
  //   client.searchColumnValuesByRowId(datasets(0), 1, 1).totalHits should be (3)
  //   client.searchColumnValuesByRowId(datasets(1), 2, 1).totalHits should be (3)

  //   client.deleteColumnValuesByRowId(datasets(0), 2, 1, refresh=true)

  //   client.searchColumnValuesByRowId(datasets(0), 2, 1).totalHits should be (0)
  //   client.searchColumnValuesByRowId(datasets(0), 2, 2).totalHits should be (3)
  //   client.searchColumnValuesByRowId(datasets(0), 1, 1).totalHits should be (3)
  //   client.searchColumnValuesByRowId(datasets(1), 2, 1).totalHits should be (3)
  // }

  test("Copy column values from one dataset copy to another") {
    val from = DatasetCopy("copy-test", 1, 2, LifecycleStage.Published)
    val to = DatasetCopy("copy-test", 2, 3, LifecycleStage.Unpublished)

    val toCopy = for {
      col <- 1 to 10
      row <- 1 to 10
    } yield ColumnValue(from.datasetId, from.copyNumber, col, s"$col|$row", 1)

    val inserts = toCopy.map(client.columnValueUpsertRequest)
    client.sendBulkRequest(inserts, refresh = true)

    client.searchColumnValuesByCopyNumber(from.datasetId, from.copyNumber).totalHits should be (100)
    client.searchColumnValuesByCopyNumber(to.datasetId, to.copyNumber).totalHits should be (0)

    client.copyColumnValues(from, to, refresh = true)

    client.searchColumnValuesByCopyNumber(from.datasetId, from.copyNumber).totalHits should be (100)
    client.searchColumnValuesByCopyNumber(to.datasetId, to.copyNumber).totalHits should be (100)
  }

  test("Search lots of column maps by copy number") {
    // Make sure that searchLotsOfColumnMapsByCopyNumber returns more than the standard
    // 10-result page of data. Start by creating column maps for a very wide dataset.
    val columns = (1 to 1000).map { idx => ColumnMap("wide-dataset", 1, idx, "col" + idx) }
    columns.foreach(client.putColumnMap(_, refresh = false))
    client.refresh()

    val retrieved = client.searchLotsOfColumnMapsByCopyNumber("wide-dataset", 1)
    retrieved.thisPage.map(_.result).sortBy(_.systemColumnId) should be (columns)
  }

  test("Put, get and delete column map") {
    client.fetchColumnMap(datasets(0), 1, "col1-1111") should not be 'defined
    client.fetchColumnMap(datasets(0), 1, "col2-2222") should not be 'defined

    val colMap = ColumnMap(datasets(0), 1, 1, "col1-1111")
    client.putColumnMap(colMap, refresh = true)

    val colMap1 = client.fetchColumnMap(datasets(0), 1, "col1-1111")
    colMap1 should be (Some(colMap))
    val colMap2 = client.fetchColumnMap(datasets(0), 1, "col2-2222")
    colMap2 should not be 'defined

    client.deleteColumnMap(colMap.datasetId, colMap.copyNumber, colMap.userColumnId, refresh = false)

    client.fetchColumnMap(datasets(0), 1, "col1-1111") should not be 'defined
  }

  test("Delete column map by dataset") {
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (9)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)

    client.deleteColumnMapsByDataset(datasets(0), refresh=true)

    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)
  }

  test("Delete column map by copy number") {
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)

    client.deleteColumnMapsByCopyNumber(datasets(0), 1, refresh=true)

    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (0)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
  }

  test("Put, get, search, delete dataset copies") {
    val dsCopies = client.searchCopiesByDataset(datasets(0))
    dsCopies.totalHits should be (3)
    dsCopies.thisPage.map(_.result).sortBy(_.copyNumber) should be (copies(datasets(0)))

    client.datasetCopy(datasets(0), 3) should be (Some(copies(datasets(0))(2)))
    client.datasetCopy(datasets(0), 4) should not be 'defined

    client.putDatasetCopy(datasets(0), 4, 20, LifecycleStage.Unpublished, refresh = true)
    client.datasetCopy(datasets(0), 4) should be
      (Some(DatasetCopy(datasets(0), 4, 20, LifecycleStage.Unpublished)))

    client.deleteDatasetCopy(datasets(0), 4, refresh = true)
    client.datasetCopy(datasets(0), 4) should not be 'defined
    client.searchCopiesByDataset(datasets(0)).totalHits should be (3)

    client.deleteDatasetCopiesByDataset(datasets(0), refresh = true)
    client.searchCopiesByDataset(datasets(0)).totalHits should be (0)
  }

  test("Get latest copy of dataset") {
    client.datasetCopyLatest(datasets(0)) should be ('defined)
    client.datasetCopyLatest(datasets(0)).get.copyNumber should be (3)

    client.datasetCopyLatest("foo") should not be 'defined
  }

  test("Get latest copy of dataset by stage") {
    client.datasetCopyLatest(datasets(0), Some(Unpublished)) should be ('defined)
    client.datasetCopyLatest(datasets(0), Some(Unpublished)).get.copyNumber should be (3)
    client.datasetCopyLatest(datasets(0), Some(Published)) should be ('defined)
    client.datasetCopyLatest(datasets(0), Some(Published)).get.copyNumber should be (2)
    client.datasetCopyLatest(datasets(0), Some(Snapshotted)) should be ('defined)
    client.datasetCopyLatest(datasets(0), Some(Snapshotted)).get.copyNumber should be (1)
  }

  test("Update dataset copy version") {
    client.putDatasetCopy(datasets(1), 1, 2, LifecycleStage.Unpublished, refresh = true)

    val current = client.datasetCopy(datasets(1), 1)
    current should be ('defined)
    current.get.version should be (2)
    current.get.stage should be (LifecycleStage.Unpublished)

    client.updateDatasetCopyVersion(
      current.get.copy(version = 5, stage = LifecycleStage.Published),
      refresh = true)

    client.datasetCopy(datasets(1), 1) should be ('defined)
    client.datasetCopy(datasets(1), 1).get.version should be (5)
    client.datasetCopy(datasets(1), 1).get.stage should be (LifecycleStage.Published)
  }

  test("Delete dataset copy by copy number") {
    client.putDatasetCopy(datasets(0), 1, 50L, LifecycleStage.Unpublished, refresh = true)
    client.putDatasetCopy(datasets(0), 2, 100L, LifecycleStage.Published, refresh = true)

    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should be ('defined)

    client.deleteDatasetCopy(datasets(0), 2, refresh = true)

    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should not be ('defined)
  }

  test("Get latest copy of Stage Number(n) should throw") {
    a[IllegalArgumentException] shouldBe thrownBy {
      client.datasetCopyLatest(datasets(0), Some(Number(42))).get.copyNumber
    }
  }

  test("Do not index empty column values") {
    var columnValue = ColumnValue(datasets(0), 61L, 1L, "", 1L)
    client.putColumnValues(datasets(0), 61L, List(columnValue))
    client.fetchColumnValue(columnValue) should be(None)

    columnValue = ColumnValue(datasets(0), 61L, 1L, " ", 1L)
    client.putColumnValues(datasets(0), 61L, List(columnValue))
    client.fetchColumnValue(columnValue) should be(None)
  }

  test("Get a dataset's copies by stage") {
    client.datasetCopiesByStage(datasets(0), Snapshotted) should be (
      List(DatasetCopy(datasets(0), 1, 5, LifecycleStage.Snapshotted)))
    client.datasetCopiesByStage(datasets(0), Unpublished) should be (
      List(DatasetCopy(datasets(0), 3, 15, LifecycleStage.Unpublished)))
  }

  test("Delete all documents associated with a dataset and return counts of types deleted") {
    client.putDatasetCopy(datasets(0), 1, 1L, LifecycleStage.Published, refresh = true)
    client.datasetCopy(datasets(0), 1) shouldBe defined
    client.refresh()
    client.deleteDatasetById(datasets(0), refresh = true) should be(
      Map("column" -> 9, "dataset_copy" -> 3, "column_value" -> 45))
    client.datasetCopy(datasets(0), 1) shouldBe None
  }
}
