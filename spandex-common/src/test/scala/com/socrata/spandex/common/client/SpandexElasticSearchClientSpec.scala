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

  def verifyFieldValue(fieldValue: FieldValue): Option[FieldValue] =
    client.client
      .prepareGet(indexName, FieldValueType, fieldValue.docId)
      .execute.actionGet
      .result[FieldValue]

  test("Insert, update, get field values, then delete and get again") {
    val toInsert = Seq(
      FieldValue("alpha.1337", 1, 20, 32, "axolotl"),
      FieldValue("alpha.1337", 1, 21, 32, "amphibious"),
      FieldValue("alpha.1337", 1, 22, 32, "Henry"))

    toInsert.foreach { fv =>
      verifyFieldValue(fv) should not be 'defined
    }

    val inserts = toInsert.map(client.fieldValueIndexRequest)
    client.sendBulkRequest(inserts, refresh = true)

    toInsert.foreach { fv =>
      verifyFieldValue(fv) should be (Some(fv))
    }

    val toUpdate = Seq(
      FieldValue("alpha.1337", 1, 20, 32, "Mexican axolotl"),
      FieldValue("alpha.1337", 1, 22, 32, "Enrique"))

    val updates = toUpdate.map(client.fieldValueUpdateRequest)
    client.sendBulkRequest(updates, refresh = true)

    verifyFieldValue(toInsert(0)).get should be (toUpdate(0))
    verifyFieldValue(toInsert(1)).get should be (toInsert(1))
    verifyFieldValue(toInsert(2)).get should be (toUpdate(1))

    val deletes = toUpdate.map(fv =>
      client.fieldValueDeleteRequest(fv.datasetId, fv.copyNumber, fv.columnId, fv.rowId))
    client.sendBulkRequest(deletes, refresh = true)

    verifyFieldValue(toInsert(0)) should not be 'defined
    verifyFieldValue(toInsert(1)).get should be (toInsert(1))
    verifyFieldValue(toInsert(2)) should not be 'defined
  }

  test("Don't send empty bulk requests to Elastic Search") {
    val empty = Seq.empty[IndexRequestBuilder]
    // BulkRequest.validate throws if given an empty bulk request
    client.sendBulkRequest(empty, refresh = true)
  }

  test("Delete field values by dataset") {
    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (45)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (45)

    client.deleteFieldValuesByDataset(datasets(0))

    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (45)
  }

  test("Delete field values by copy number") {
    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)

    client.deleteFieldValuesByCopyNumber(datasets(0), 2)

    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
  }

  test("Delete field values by column") {
    client.searchFieldValuesByColumnId(datasets(0), 1, 1).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, 1).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, 2).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(1), 2, 1).totalHits should be (5)

    client.deleteFieldValuesByColumnId(datasets(0), 2, 1)

    client.searchFieldValuesByColumnId(datasets(0), 1, 1).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, 1).totalHits should be (0)
    client.searchFieldValuesByColumnId(datasets(0), 2, 2).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(1), 2, 1).totalHits should be (5)
  }

  test("Delete field values by row") {
    client.searchFieldValuesByRowId(datasets(0), 2, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 2, 2).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 1, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(1), 2, 1).totalHits should be (3)

    client.deleteFieldValuesByRowId(datasets(0), 2, 1)

    client.searchFieldValuesByRowId(datasets(0), 2, 1).totalHits should be (0)
    client.searchFieldValuesByRowId(datasets(0), 2, 2).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 1, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(1), 2, 1).totalHits should be (3)
  }

  test("Copy field values from one dataset copy to another") {
    val from = DatasetCopy("copy-test", 1, 2, LifecycleStage.Published)
    val to = DatasetCopy("copy-test", 2, 3, LifecycleStage.Unpublished)

    val toCopy = for {
      col <- 1 to 10
      row <- 1 to 10
    } yield FieldValue(from.datasetId, from.copyNumber, col, row, s"$col|$row")

    val inserts = toCopy.map(client.fieldValueIndexRequest)
    client.sendBulkRequest(inserts, refresh = true)

    client.searchFieldValuesByCopyNumber(from.datasetId, from.copyNumber).totalHits should be (100)
    client.searchFieldValuesByCopyNumber(to.datasetId, to.copyNumber).totalHits should be (0)

    client.copyFieldValues(from, to, refresh = true)

    client.searchFieldValuesByCopyNumber(from.datasetId, from.copyNumber).totalHits should be (100)
    client.searchFieldValuesByCopyNumber(to.datasetId, to.copyNumber).totalHits should be (100)
  }

  test("Search lots of column maps by copy number") {
    // Make sure that searchLotsOfColumnMapsByCopyNumber returns more than the standard
    // 10-result page of data. Start by creating column maps for a very wide dataset.
    val columns = (1 to 1000).map { idx => ColumnMap("wide-dataset", 1, idx, "col" + idx) }
    columns.foreach(client.putColumnMap(_, refresh = false))
    client.refresh()

    val retrieved = client.searchLotsOfColumnMapsByCopyNumber("wide-dataset", 1)
    retrieved.thisPage.sortBy(_.systemColumnId) should be (columns)
  }

  test("Put, get and delete column map") {
    client.columnMap(datasets(0), 1, "col1-1111") should not be 'defined
    client.columnMap(datasets(0), 1, "col2-2222") should not be 'defined

    val colMap = ColumnMap(datasets(0), 1, 1, "col1-1111")
    client.putColumnMap(colMap, refresh = true)

    val colMap1 = client.columnMap(datasets(0), 1, "col1-1111")
    colMap1 should be (Some(colMap))
    val colMap2 = client.columnMap(datasets(0), 1, "col2-2222")
    colMap2 should not be 'defined

    client.deleteColumnMap(colMap.datasetId, colMap.copyNumber, colMap.userColumnId)

    client.columnMap(datasets(0), 1, "col1-1111") should not be 'defined
  }

  test("Delete column map by dataset") {
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (9)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)

    client.deleteColumnMapsByDataset(datasets(0))

    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)
  }

  test("Delete column map by copy number") {
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)

    client.deleteColumnMapsByCopyNumber(datasets(0), 1)

    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (0)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
  }

  test("Put, get, search, delete dataset copies") {
    val dsCopies = client.searchCopiesByDataset(datasets(0))
    dsCopies.totalHits should be (3)
    dsCopies.thisPage.sortBy(_.copyNumber) should be (copies(datasets(0)))

    client.datasetCopy(datasets(0), 3) should be (Some(copies(datasets(0))(2)))
    client.datasetCopy(datasets(0), 4) should not be 'defined

    client.putDatasetCopy(datasets(0), 4, 20, LifecycleStage.Unpublished, refresh = true)
    client.datasetCopy(datasets(0), 4) should be
      (Some(DatasetCopy(datasets(0), 4, 20, LifecycleStage.Unpublished)))

    client.deleteDatasetCopy(datasets(0), 4)
    client.datasetCopy(datasets(0), 4) should not be 'defined
    client.searchCopiesByDataset(datasets(0)).totalHits should be (3)

    client.deleteDatasetCopiesByDataset(datasets(0))
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

    client.deleteDatasetCopy(datasets(0), 2)

    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should not be ('defined)
  }

  test("Get latest copy of Stage Number(n) should throw") {
    a[IllegalArgumentException] shouldBe thrownBy {
      client.datasetCopyLatest(datasets(0), Some(Number(42))).get.copyNumber
    }
  }

  test("Do not index empty or null field values") {
    client.indexFieldValue(FieldValue(datasets(0), 1L, 2L, 61L, ""), refresh = true) should be(false)
    client.indexFieldValue(FieldValue(datasets(0), 1L, 2L, 61L, " "), refresh = true) should be(false)
    client.indexFieldValue(FieldValue(datasets(0), 1L, 2L, 61L, null), refresh = true) should be(false)
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
    client.deleteDatasetById(datasets(0)) should be(
      Map("column_map" -> 9, "dataset_copy" -> 3, "field_value" -> 45))
    client.datasetCopy(datasets(0), 1) shouldBe None
  }
}
