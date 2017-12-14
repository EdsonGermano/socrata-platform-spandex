package com.socrata.spandex.secondary

import java.math.BigDecimal

import org.joda.time.DateTime
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuiteLike, Matchers}

import com.socrata.datacoordinator.id.{ColumnId, CopyId, RowId, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLNumber, SoQLText, SoQLValue}
import com.socrata.spandex.common.SpandexIntegrationTest
import com.socrata.spandex.common.client.{ColumnMap, DatasetCopy}

// scalastyle:off
class VersionEventsHandlerSpec extends FunSuiteLike
  with Matchers
  with PropertyChecks
  with SpandexIntegrationTest {

  // Make batches teensy weensy to expose any batching issues
  val handler = new VersionEventsHandler(client, 2)

  test("All - throw an exception if data version is invalid") {
    val invalidDataVersion = 0
    val events = Seq(Truncated).iterator

    an [IllegalArgumentException] should be thrownBy handler.handle("alpha.75", invalidDataVersion, events)
  }

  test("WorkingCopyCreated - throw an exception if multiple working copies are encountered in the same event sequence") {
    val copyInfo = CopyInfo(new CopyId(100), 1, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo), WorkingCopyCreated(copyInfo)).iterator

    a [UnsupportedOperationException] should be thrownBy handler.handle("alpha.75", 1, events)
  }

  test("WorkingCopyCreated - throw an exception if the copy to be created already exists") {
    val dataVersion = 12
    val dataset = "alpha.75"
    val copyInfo = CopyInfo(new CopyId(100), 1, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo)).iterator

    client.putDatasetCopy(dataset, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage, refresh = true)

    a [ResyncSecondaryException] should be thrownBy handler.handle(dataset, 1, events)
  }

  test("WorkingCopyCreated - a new dataset copy should be added to the index") {
    val dataVersion = 15
    val dataset = "alpha.76"
    val copyInfo = CopyInfo(new CopyId(100), 45, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo)).iterator

    client.datasetCopy(dataset, copyInfo.copyNumber) should not be 'defined

    handler.handle(dataset, dataVersion, events)
    val maybeCopyRecord = client.datasetCopy(dataset, copyInfo.copyNumber)
    maybeCopyRecord should be ('defined)
    maybeCopyRecord.get should be (DatasetCopy(dataset, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage))
  }

  test("Truncate - field values in latest copy of dataset should be dropped") {
    val expectedLatestBefore = copies(datasets(0)).last

    val latestBefore = client.datasetCopyLatest(datasets(0))
    latestBefore should be ('defined)
    latestBefore.get.copyNumber should be (expectedLatestBefore.copyNumber)
    latestBefore.get.version should be (expectedLatestBefore.version)
    client.searchColumnValuesByCopyNumber(datasets(0), expectedLatestBefore.copyNumber).totalHits should be (15)

    handler.handle(datasets(0), expectedLatestBefore.version + 1, Seq(Truncated).iterator)

    val latestAfter = client.datasetCopyLatest(datasets(0))
    latestAfter should be ('defined)
    latestAfter.get.copyNumber should be (expectedLatestBefore.copyNumber)
    latestAfter.get.version should be (expectedLatestBefore.version + 1)
    client.searchColumnValuesByCopyNumber(datasets(0), expectedLatestBefore.copyNumber).totalHits should be (0)
  }

  test("ColumnCreated - don't create column map for non-SoQLText columns") {
    val numCol = ColumnInfo(new ColumnId(10), new UserColumnId("nums-1234"), Some(ColumnName("numbers")), SoQLNumber, false, false, false, None)
    val textCol = ColumnInfo(new ColumnId(20), new UserColumnId("text-1234"), Some(ColumnName("text")), SoQLText, false, false, false, None)

    // Create column
    handler.handle(datasets(0), 3, Seq(ColumnCreated(numCol), ColumnCreated(textCol)).iterator)

    val latest = client.datasetCopyLatest(datasets(0))
    client.fetchColumnMap(datasets(0), latest.get.copyNumber, numCol.id.underlying) should not be 'defined
    val textColMap = client.fetchColumnMap(datasets(0), latest.get.copyNumber, textCol.id.underlying)
    textColMap should be
      (Some(ColumnMap(datasets(0), latest.get.copyNumber, textCol.systemId.underlying, textCol.id.underlying)))
  }

  test("ColumnCreated and ColumnRemoved") {
    val expectedLatestBefore = copies(datasets(0)).last
    val info = ColumnInfo(new ColumnId(3), new UserColumnId("blah-1234"), Some(ColumnName("blah-field")), SoQLText, false, false, false, None)

    val latestBeforeAdd = client.datasetCopyLatest(datasets(0))
    latestBeforeAdd should be ('defined)
    latestBeforeAdd.get.copyNumber should be (expectedLatestBefore.copyNumber)
    latestBeforeAdd.get.version should be (expectedLatestBefore.version)
    client.fetchColumnMap(datasets(0), expectedLatestBefore.copyNumber, info.id.underlying) should not be 'defined

    // Create column
    handler.handle(datasets(0), expectedLatestBefore.version + 1, Seq(ColumnCreated(info)).iterator)

    client.fetchColumnMap(datasets(0), expectedLatestBefore.copyNumber, info.id.underlying) should be
      (Some(ColumnMap(datasets(0), 2, info.systemId.underlying, info.id.underlying)))

    // Pretend we added some data in between (which actually got added during bootstrap)
    client.searchColumnValuesByColumnId(datasets(0), 2, 3).totalHits should be (5)

    // Remove column
    handler.handle(datasets(0), expectedLatestBefore.version + 1, Seq(ColumnRemoved(info)).iterator)

    val latestAfterDelete = client.datasetCopyLatest(datasets(0))
    latestAfterDelete should be ('defined)
    latestAfterDelete.get.copyNumber should be (expectedLatestBefore.copyNumber)
    latestAfterDelete.get.version should be (expectedLatestBefore.version + 1)
    client.searchColumnValuesByColumnId(
      datasets(0), expectedLatestBefore.copyNumber, info.systemId.underlying).totalHits should be (0)
    client.fetchColumnMap(datasets(0), expectedLatestBefore.copyNumber, info.id.underlying) should not be 'defined
  }

  test("WorkingCopyPublished - throw exception if the current copy is not Unpublished.") {
    client.putDatasetCopy("wcp-invalid-test", 1, 2, LifecycleStage.Published, refresh = true)

    a [ResyncSecondaryException] should be thrownBy
      handler.handle("wcp-invalid-test", 3, Seq(WorkingCopyPublished).iterator)
  }

  test("WorkingCopyPublished - publish first working copy") {
    client.putDatasetCopy("wcp-first-test", 1, 2, LifecycleStage.Unpublished, refresh = true)

    handler.handle("wcp-first-test", 3, Seq(WorkingCopyPublished).iterator)

    client.datasetCopy("wcp-first-test", 1) should be
      (Some(DatasetCopy("wcp-first-test", 1, 3, LifecycleStage.Published)))
  }

  test("WorkingCopyPublished - publish subsequent working copy, set previous published copies to snapshotted") {
   client.putDatasetCopy("wcp-second-test", 1, 2, LifecycleStage.Published, refresh = true)
   client.putDatasetCopy("wcp-second-test", 2, 4, LifecycleStage.Unpublished, refresh = true)

   handler.handle("wcp-second-test", 5, Seq(WorkingCopyPublished).iterator)

   client.datasetCopy("wcp-second-test", 1) should be
     (Some(DatasetCopy("wcp-second-test", 1, 2, LifecycleStage.Snapshotted)))
   client.datasetCopy("wcp-second-test", 2) should be
     (Some(DatasetCopy("wcp-second-test", 2, 5, LifecycleStage.Published)))
  }

  test("WorkingCopyDropped - throw an exception if the copy is the initial copy") {
    client.putDatasetCopy("wcd-test-initial-copy", 1, 1, LifecycleStage.Unpublished, refresh = true)

    val expectedBefore = Some(DatasetCopy("wcd-test-initial-copy", 1, 1, LifecycleStage.Unpublished))
    client.datasetCopyLatest("wcd-test-initial-copy") should be(expectedBefore)

    an [InvalidStateBeforeEvent] should be thrownBy
      handler.handle("wcd-test-initial-copy", 2, Seq(WorkingCopyDropped).iterator)
  }

  test("WorkingCopyDropped - throw an exception if the copy is in the wrong stage") {
    client.putDatasetCopy("wcd-test-published", 2, 2, LifecycleStage.Published, refresh = true)

    val expectedBefore = Some(DatasetCopy("wcd-test-published", 2, 2, LifecycleStage.Published))
    client.datasetCopyLatest("wcd-test-published") should be(expectedBefore)

    an [InvalidStateBeforeEvent] should be thrownBy
      handler.handle("wcd-test-published", 3, Seq(WorkingCopyDropped).iterator)
  }

  test("WorkingCopyDropped - latest working copy should be dropped") {
    val expectedBefore = copies(datasets(1)).last
    client.datasetCopyLatest(datasets(1)) should be(Some(expectedBefore))
    client.searchColumnValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 3).totalHits should be (15)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 2).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 3).totalHits should be (3)

    handler.handle(datasets(1), expectedBefore.version + 1, Seq(WorkingCopyDropped).iterator)

    val expectedAfter = copies(datasets(1))(1).copy(version = expectedBefore.version + 1)
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedAfter))
    client.searchColumnValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 3).totalHits should be (0)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 2).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 3).totalHits should be (0)
  }

  test("SnapshotDropped - throw an exception if the copy is in the wrong stage") {
    client.putDatasetCopy("sd-test-notsnapshot", 2, 2, LifecycleStage.Unpublished, refresh = true)

    val expectedBefore = Some(DatasetCopy("sd-test-notsnapshot", 2, 2, LifecycleStage.Unpublished))
    client.datasetCopyLatest("sd-test-notsnapshot") should be (expectedBefore)

    val copyInfo = CopyInfo(new CopyId(100), 2, LifecycleStage.Unpublished, 2, DateTime.now)
    val events =  Seq(SnapshotDropped(copyInfo)).iterator
    an [InvalidStateBeforeEvent] should be thrownBy handler.handle("sd-test-notsnapshot", 3, events)
  }

  test("SnapshotDropped - the specified snapshot should be dropped") {
    val expectedBefore = copies(datasets(1)).last
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedBefore))
    client.searchColumnValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 3).totalHits should be (15)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 2).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 3).totalHits should be (3)

    val snapshot = copies(datasets(1)).head
    val copyInfo = CopyInfo(new CopyId(100), snapshot.copyNumber, snapshot.stage, snapshot.version, DateTime.now)
    handler.handle(datasets(1), expectedBefore.version + 1, Seq(SnapshotDropped(copyInfo)).iterator)

    val expectedAfter = expectedBefore.copy(version = expectedBefore.version + 1)
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedAfter))
    client.searchColumnValuesByCopyNumber(datasets(1), 1).totalHits should be (0)
    client.searchColumnValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 3).totalHits should be (15)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (0)
    client.searchColumnMapsByCopyNumber(datasets(1), 2).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 3).totalHits should be (3)
  }

  test("RowDataUpdated - Insert and update") {
    val expectedBeforeInsert = copies(datasets(1)).last
    val insert = Insert(new RowId(6), ColumnIdMap[SoQLValue](
                          new ColumnId(5) -> SoQLText("index me!"),
                          new ColumnId(9) -> SoQLNumber(new BigDecimal(5))))

    client.datasetCopyLatest(datasets(1)) should be (Some(expectedBeforeInsert))
    client.searchColumnValuesByCopyNumber(
      datasets(1), expectedBeforeInsert.copyNumber).totalHits should be (15)

    val insertEvents = Seq(RowDataUpdated(Seq[Operation](insert))).iterator
    handler.handle(datasets(1), expectedBeforeInsert.version + 1, insertEvents)

    val expectedAfterInsert = expectedBeforeInsert.copy(version = expectedBeforeInsert.version + 1)
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedAfterInsert))
    client.searchColumnValuesByCopyNumber(
      datasets(1), expectedBeforeInsert.copyNumber).totalHits should be (16)

    val oldData = ColumnIdMap[SoQLValue](
      new ColumnId(2) -> SoQLText("data column 2 row 2"),
      new ColumnId(3) -> SoQLText("data column 3 row 2")
    )

    val update = Update(
      new RowId(2),
      ColumnIdMap[SoQLValue](
        new ColumnId(2) -> SoQLText("updated data2"),
        new ColumnId(3) -> SoQLText("updated data3")
      )
    )(Some(oldData))

    val updateEvents = Seq(RowDataUpdated(Seq[Operation](update))).iterator
    handler.handle(datasets(1), expectedAfterInsert.version + 1, updateEvents)

    val expectedAfter = expectedAfterInsert.copy(version = expectedAfterInsert.version + 1)
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedAfter))
    client.searchColumnValuesByCopyNumber(
      datasets(1), expectedAfterInsert.copyNumber).totalHits should be (16)
  }

  test("RowDataUpdated - Delete") {
    val expectedBefore = copies(datasets(1)).last
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedBefore))
    client.searchColumnValuesByCopyNumber(datasets(1), expectedBefore.copyNumber).totalHits should be (15)

    val oldDataRow2 = ColumnIdMap[SoQLValue](
      new ColumnId(1) -> SoQLText("data column 1 row 2"),
      new ColumnId(2) -> SoQLText("data column 2 row 2"),
      new ColumnId(3) -> SoQLText("data column 3 row 2")
    )
    
    val oldDataRow5 = ColumnIdMap[SoQLValue](
      new ColumnId(1) -> SoQLText("data column 1 row 5"),
      new ColumnId(2) -> SoQLText("data column 2 row 5"),
      new ColumnId(3) -> SoQLText("data column 3 row 5")
    )

    val events = Seq(
      RowDataUpdated(
        Seq[Operation](
          Delete(new RowId(2))(Some(oldDataRow2)),
          Delete(new RowId(5))(Some(oldDataRow5))
        )
      )
    ).iterator

    handler.handle(datasets(1), expectedBefore.version + 1, events)

    val expectedAfter = expectedBefore.copy(version = expectedBefore.version + 1)
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedAfter))
    client.searchColumnValuesByCopyNumber(datasets(1), expectedAfter.copyNumber).totalHits should be (9)
  }

  test("RowDataUpdated - operations get executed in the right order") {
    // Add column mappings for imaginary columns
    client.putDatasetCopy("fun-with-ordering", 1, 1, LifecycleStage.Unpublished, refresh = true)
    client.putColumnMap(ColumnMap("fun-with-ordering", 1, 50, "myco-l050"), refresh = true)
    client.putColumnMap(ColumnMap("fun-with-ordering", 1, 51, "myco-l051"), refresh = true)

    // Row 1 - we'll insert it first, then delete it.
    // We expect it not to exist at the end of the test.
    val row1Data = ColumnIdMap[SoQLValue](
      new ColumnId(50) -> SoQLText("1.50#1"),
      new ColumnId(51) -> SoQLText("1.51#1")
    )

    val row1Insert = Insert(new RowId(100), row1Data)
    val row1Delete = Delete(new RowId(100))(Some(row1Data))

    // Row 2 - we'll delete it first, then insert it and update it several times.
    // We expect it to exist in its most up to date form at the end of the test.
    val row2Data = ColumnIdMap[SoQLValue](
      new ColumnId(50) -> SoQLText("2.50#1"),
      new ColumnId(51) -> SoQLText("2.51#1")
    )

    val row2Delete = Delete(new RowId(101))(Some(row2Data))
    val row2Insert = Insert(new RowId(101), row2Data)

    val row2Update1Data = ColumnIdMap[SoQLValue](
      new ColumnId(50) -> SoQLText("2.50#2"),
      new ColumnId(51) -> SoQLText("2.51#2")
    )

    val row2Update1 = Update(new RowId(101), row2Update1Data)(Some(row2Data))

    val row2Update2 = Update(
      new RowId(101),
      ColumnIdMap[SoQLValue](
        new ColumnId(50) -> SoQLText("2.50#3"),
        new ColumnId(51) -> SoQLText("2.51#3")
      )
    )(Some(row2Update1Data))

    val events = Seq(RowDataUpdated(Seq[Operation](
      row2Delete, row1Insert, row2Insert, row1Delete, row2Update1, row2Update2)))
    handler.handle("fun-with-ordering", 2, events.iterator)

    val columnValues = client.searchColumnValuesByCopyNumber("fun-with-ordering", 1).thisPage.map(_.result)
    val row1Values = columnValues.filter(_.value.startsWith("1."))
    row1Values.size should be (0)
    val row2Values = columnValues.filter(_.value.startsWith("2.")).sortBy(_.value)
    row2Values.map(_.value) should contain theSameElementsInOrderAs(List("2.50#3", "2.51#3"))
  }

  test("DataCopied - all field values from last published copy should be copied to latest copy") {
    // Remove bootstrapped data on working copy
    client.deleteColumnValuesByCopyNumber(datasets(1), 3, refresh = true)

    val expectedBefore = copies(datasets(1)).last
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedBefore))
    client.searchColumnValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 3).totalHits should be (0)

    handler.handle(datasets(1), expectedBefore.version + 1, Seq(DataCopied).iterator)

    val expectedAfter = expectedBefore.copy(version = expectedBefore.version + 1)
    client.datasetCopyLatest(datasets(1)) should be (Some(expectedAfter))
    client.searchColumnValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(1), 3).totalHits should be (15)
  }

  // once upon a time we got this exception:
  // org.elasticsearch.index.mapper.MapperParsingException: failed to parse
  // Cause: java.lang.IllegalArgumentException: surface form cannot contain unit separator character U+001F; this character is reserved
  // which seems to come from data on this profile page https://twitter.com/kashiramojiao
  // ヲタでネトウヨで女装子でアラフォーのキモいおっさんなのん
  // (((o(´▽`)o)))
  // I,m Ｊａｐａｎｅｓｅ　Otaku and Middle-aged man　and Patriot　and 　Conservatism.
  // My Tweets combines the honesty and odiousness
  test("strip value containing reserved control character x1F") {
    val cv = RowOpsHandler.columnValueFromDatum(datasets(0), 1L, (new ColumnId(2L), new SoQLText("(((o(\u001F´▽`\u001F)o)))")))
    client.indexColumnValue(cv)
    client.fetchColumnValueByDocid(cv.docId).value should be("(((o(´▽`)o)))")
  }
}
