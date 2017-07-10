package com.socrata.spandex.secondary

import java.math.BigDecimal

import com.rojoma.simplearm.util.unmanaged
import com.socrata.datacoordinator.id.{ColumnId, CopyId, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.spandex.common._
import com.socrata.spandex.common.client.{ColumnMap, DatasetCopy, FieldValue, TestESClient}
import com.socrata.spandex.common.client.SpandexElasticSearchClient
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

class TestSpandexSecondary(config: ElasticSearchConfig, testClient: TestESClient) extends SpandexSecondaryLike {
  val client = testClient
  val index = config.index
  val batchSize = config.dataCopyBatchSize

  def shutdown(): Unit = client.close()
}

// scalastyle:off
class SpandexSecondaryLikeSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterEach with BeforeAndAfterAll {
  val config = new SpandexConfig

  val indexName = getClass.getSimpleName.toLowerCase
  val testClient = new TestESClient(indexName)
  val secondary = new TestSpandexSecondary(config.es, testClient)

  def client = secondary.client

  override protected def beforeAll(): Unit =
    SpandexElasticSearchClient.ensureIndex(config.es.index, client)

  override def beforeEach(): Unit = bootstrapData()

  override def afterEach(): Unit = removeBootstrapData()

  test("drop dataset") {
    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (45)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (45)
    client.searchCopiesByDataset(datasets(0)).totalHits should be (3)
    client.searchCopiesByDataset(datasets(1)).totalHits should be (3)
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (9)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)

    secondary.dropDataset(datasets(0), None)

    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (45)
    client.searchCopiesByDataset(datasets(0)).totalHits should be (0)
    client.searchCopiesByDataset(datasets(1)).totalHits should be (3)
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)
  }

  test("drop copy") {
    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (15)
    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should be ('defined)
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(0), 2).totalHits should be (3)

    val datasetInfo = DatasetInfo(datasets(0), "en-US", Array.empty)
    val copyInfo = CopyInfo(new CopyId(100), 2, LifecycleStage.Published, 10, DateTime.now)
    secondary.dropCopy(datasetInfo, copyInfo, None, isLatestCopy = true)

    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should not be 'defined
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(0), 2).totalHits should be (0)
  }

  test("drop future copy") {
    client.datasetCopy(datasets(0), 7) should not be 'defined
    val datasetInfo = DatasetInfo(datasets(0), "en-US", Array.empty)
    val copyInfo = CopyInfo(new CopyId(200), 7, LifecycleStage.Published, 77, DateTime.now)
    secondary.dropCopy(datasetInfo, copyInfo, None, isLatestCopy = true)
  }

  test("resync") {
    // Add some stale data related to this dataset, which should be cleaned up in the resync
    client.indexFieldValue(FieldValue("zoo-animals", 5, 3, 6, "marmot"), refresh = true)
    client.putColumnMap(ColumnMap("zoo-animals", 5, 3, "species"), refresh = true)
    client.putDatasetCopy("zoo-animals", 5, 5, LifecycleStage.Unpublished, refresh = true)

    client.searchCopiesByDataset("zoo-animals").totalHits should be (1)
    client.searchColumnMapsByDataset("zoo-animals").totalHits should be (1)
    client.searchFieldValuesByDataset("zoo-animals").totalHits should be (1)

    val datasetInfo = DatasetInfo("zoo-animals", "en-US", Array.empty)
    val copyInfo = CopyInfo(new CopyId(100), 5, LifecycleStage.Published, 15, DateTime.now)
    val schema = ColumnIdMap[ColumnInfo[SoQLType]](
      new ColumnId(1) -> ColumnInfo[SoQLType](new ColumnId(1), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None),
      new ColumnId(2) -> ColumnInfo[SoQLType](new ColumnId(2), new UserColumnId("animal"), Some(ColumnName("animal")), SoQLText, false, false, false, None),
      new ColumnId(3) -> ColumnInfo[SoQLType](new ColumnId(3), new UserColumnId("class"), Some(ColumnName("class")), SoQLText, false, false, false, None),
      new ColumnId(4) -> ColumnInfo[SoQLType](new ColumnId(4), new UserColumnId("population"), Some(ColumnName("population")), SoQLNumber, false, false, false, None)
    )
    val rows = Seq(
      ColumnIdMap[SoQLValue](
        new ColumnId(1) -> new SoQLID(10),
        new ColumnId(2) -> new SoQLText("giraffe"),
        new ColumnId(3) -> new SoQLText("mammalia"),
        new ColumnId(4) -> new SoQLNumber(new BigDecimal(6))),
      ColumnIdMap[SoQLValue](
        new ColumnId(1) -> new SoQLID(12),
        new ColumnId(2) -> new SoQLText("axolotl"),
        new ColumnId(3) -> new SoQLText("amphibia"),
        new ColumnId(4) -> new SoQLNumber(new BigDecimal(2))),
      ColumnIdMap[SoQLValue](
        new ColumnId(1) -> new SoQLID(14),
        new ColumnId(2) -> new SoQLText("ostrich"),
        new ColumnId(3) -> new SoQLText("avia"),
        new ColumnId(4) -> new SoQLNumber(new BigDecimal(3)))
    )

    secondary.resync(datasetInfo, copyInfo, schema, None, unmanaged(rows.iterator), Seq.empty, isLatestLivingCopy = true)

    val copies = client.searchCopiesByDataset("zoo-animals")
    copies.totalHits should be (1)
    copies.thisPage(0) should be (DatasetCopy("zoo-animals",
                                              copyInfo.copyNumber,
                                              copyInfo.dataVersion,
                                              copyInfo.lifecycleStage))

    client.searchColumnMapsByDataset("zoo-animals").totalHits should be (2)
    val columnMaps = client.searchColumnMapsByDataset("zoo-animals").thisPage.sortBy(_.systemColumnId)
    columnMaps(0) should be (ColumnMap("zoo-animals", copyInfo.copyNumber, 2, "animal"))
    columnMaps(1) should be (ColumnMap("zoo-animals", copyInfo.copyNumber, 3, "class"))

    client.searchFieldValuesByDataset("zoo-animals").totalHits should be (6)
    val fieldValues = client.searchFieldValuesByDataset("zoo-animals").thisPage.sortBy(fv => (fv.rowId, fv.columnId))
    fieldValues(0) should be (FieldValue("zoo-animals", copyInfo.copyNumber, 2, 10, "giraffe"))
    fieldValues(1) should be (FieldValue("zoo-animals", copyInfo.copyNumber, 3, 10, "mammalia"))
    fieldValues(2) should be (FieldValue("zoo-animals", copyInfo.copyNumber, 2, 12, "axolotl"))
    fieldValues(3) should be (FieldValue("zoo-animals", copyInfo.copyNumber, 3, 12, "amphibia"))
    fieldValues(4) should be (FieldValue("zoo-animals", copyInfo.copyNumber, 2, 14, "ostrich"))
    fieldValues(5) should be (FieldValue("zoo-animals", copyInfo.copyNumber, 3, 14, "avia"))
  }
}
