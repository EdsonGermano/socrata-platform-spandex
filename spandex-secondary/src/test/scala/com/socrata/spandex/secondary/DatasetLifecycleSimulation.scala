package com.socrata.spandex.secondary

import java.math.BigDecimal

import com.socrata.datacoordinator.id.{ColumnId, CopyId, RowId, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.scalatest.{FunSuiteLike, Matchers}

import com.socrata.spandex.common.SpandexConfig
import com.socrata.spandex.common.client.{ColumnMap, DatasetCopy, FieldValue, TestESClient}

/**
 * This test is as much documentation as it is a test.
 * Namely, it documents how dataset API operations map to secondary events.
 * The test was constructed by manually calling various Soda Fountain endpoints
 * and capturing the batches of events that were passed to spandex secondary.
 * This test aims to be a good starting point to understand how the spandex secondary works.
 */
// scalastyle:off
class DatasetLifecycleSimulation extends FunSuiteLike with Matchers {
  val config = new SpandexConfig(ConfigFactory.load().getConfig("com.socrata.spandex")
    .withValue("elastic-search.index", ConfigValueFactory.fromAnyRef("spandex-dataset-lifecycle")))

  val indexName = getClass.getSimpleName.toLowerCase
  val client = new TestESClient(indexName)

  val secondary = new TestSpandexSecondary(config.es, client)

  test("Entire lifecycle of a dataset") {
    val dataset = DatasetInfo(
      internalName =  "my-so-called-lifecycle",
      localeName = "en-US",
      obfuscationKey = Array.empty[Byte])

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // DATASET CREATION
    // Soda Fountain API: POST /dataset
    // Resulting secondary events:
    // - WorkingCopyCreated event
    // - One ColumnCreated event for the addition of each system column - :id, :version, :created_at, :updated_at
    // - VersionColumnChanged event designating :version as the default version column
    // - SystemRowIdentifierChanged event designating :id as the default system row identifier
    // - LastModifiedChanged event - sent along with any change to the dataset
    ////////////////////////////////////////////////////////////////////////////////////////////////
    val datasetCreateEvents = locally {
      val workingCopyCreated = WorkingCopyCreated(CopyInfo(
        systemId = new CopyId(296),
        copyNumber = 1,
        lifecycleStage = LifecycleStage.Unpublished,
        dataVersion = 0,
        lastModified = DateTime.now))

      val addUpdatedAtColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(0),
        id = new UserColumnId(":updated_at"),
        fieldName = Some(ColumnName(":updated_at")),
        typ = SoQLFixedTimestamp,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val addVersionColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(1),
        id = new UserColumnId(":version"),
        fieldName = Some(ColumnName(":version")),
        typ = SoQLVersion,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val addIdColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(2),
        id = new UserColumnId(":id"),
        fieldName = Some(ColumnName(":id")),
        typ = SoQLID,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val addCreatedAtColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(3),
        id = new UserColumnId(":created_at"),
        fieldName = Some(ColumnName(":created_at")),
        typ = SoQLFixedTimestamp,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val versionColumnChanged = VersionColumnChanged(ColumnInfo(
        systemId = new ColumnId(1),
        id = new UserColumnId(":version"),
        fieldName = Some(ColumnName(":version")),
        typ = SoQLVersion,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = true,
        computationStrategyInfo = None))

      val systemRowIdChanged = SystemRowIdentifierChanged(ColumnInfo(
        systemId = new ColumnId(2),
        id = new UserColumnId(":id"),
        fieldName = Some(ColumnName(":id")),
        typ = SoQLID,
        isSystemPrimaryKey = true,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val lastModifiedChanged = LastModifiedChanged(DateTime.now)

      Iterator(workingCopyCreated, addUpdatedAtColumn, addVersionColumn, addIdColumn, addCreatedAtColumn,
        versionColumnChanged, systemRowIdChanged, lastModifiedChanged)
    }

    // Secondary processes its first ever events for this dataset
    secondary.version(dataset, 1, None, datasetCreateEvents)

    // We should now see a corresponding dataset_copy doc in Elastic Search
    client.datasetCopy(dataset.internalName, 1) should be ('defined)
    client.datasetCopy(dataset.internalName, 1).get should be
      (DatasetCopy(dataset.internalName, 1, 1, LifecycleStage.Unpublished))
    // And no column_map docs yet, because we don't put system columns in Spandex
    client.searchColumnMapsByCopyNumber(dataset.internalName, 1).totalHits should be (0)
    // And no field_value docs yet, because, we have no rows
    client.searchFieldValuesByCopyNumber(dataset.internalName, 1).totalHits should be (0)


    ////////////////////////////////////////////////////////////////////////////////////////////////
    // COLUMN CREATION
    // Soda Fountain API: POST /dataset/{resource_name}/{column_name}
    // Resulting secondary events:
    // - ColumnCreated event
    // - LastModifiedChanged event - sent along with any change to the dataset
    ////////////////////////////////////////////////////////////////////////////////////////////////
    val addTextColumnEvents = locally {
      val addTextColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(4),
        id = new UserColumnId("ggha-z6i9"),
        fieldName = Some(ColumnName("text-field")),
        typ = SoQLText,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val lastModifiedChanged = LastModifiedChanged(DateTime.now)

      Iterator(addTextColumn, lastModifiedChanged)
    }

    // Secondary processes the add column events
    secondary.version(dataset, 2, None, addTextColumnEvents)

    // We should now see a corresponding column_map doc in Elastic Search
    client.searchColumnMapsByCopyNumber(dataset.internalName, 1).totalHits should be (1)
    client.searchColumnMapsByCopyNumber(dataset.internalName, 1).thisPage.head should be
      (ColumnMap(dataset.internalName, 1, 4, "ggha-z6i9"))
    // The current copy should be bumped to the latest data version
    client.datasetCopy(dataset.internalName, 1).get should be
      (DatasetCopy(dataset.internalName, 1, 2, LifecycleStage.Unpublished))

    // Spandex only indexes text columns.
    // If I add a non-text column, it won't be indexed.
    val addNumberColumnEvents = locally {
      val addNumberColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(5),
        id = new UserColumnId("j9bp-sh9q"),
        fieldName = Some(ColumnName("number-field")),
        typ = SoQLNumber,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val lastModifiedChanged = LastModifiedChanged(DateTime.now)

      Iterator(addNumberColumn, lastModifiedChanged)
    }

    // Secondary processes the add column events
    secondary.version(dataset, 3, None, addNumberColumnEvents)

    // We should NOT see a corresponding column_map doc in Elastic Search
    client.columnMap(dataset.internalName, 1, "j9bp-sh9q") should not be 'defined
    // BUT the current copy should still be bumped to the latest data version
    client.datasetCopy(dataset.internalName, 1).get should be
      (DatasetCopy(dataset.internalName, 1, 3, LifecycleStage.Unpublished))

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // ROW OPERATIONS
    // Soda Fountain API: POST /resource/{resource_name}
    //                    POST /resource/{resource_name}/{row_specifier}
    // Resulting secondary events:
    // - RowDataUpdated event
    // - LastModifiedChanged event - sent along with any change to the dataset
    ////////////////////////////////////////////////////////////////////////////////////////////////
    val rowOperationsEvents = locally {
      val inserts = (1 to 10).map { i =>
        Insert(new RowId(i), ColumnIdMap[SoQLValue](
          new ColumnId(4) -> SoQLText("giraffe " + i), new ColumnId(5) -> SoQLNumber(new BigDecimal(5))))
      }
      val rowDataUpdated = RowDataUpdated(inserts)

      val lastModifiedChanged = LastModifiedChanged(DateTime.now)

      Iterator(rowDataUpdated, lastModifiedChanged)
    }

    // Secondary processes the row operation events
    secondary.version(dataset, 4, None, rowOperationsEvents)

    // We should see 10 new field_value docs in Elastic Search
    client.searchFieldValuesByCopyNumber(dataset.internalName, 1).totalHits should be (10)
    client.searchFieldValuesByCopyNumber(dataset.internalName, 1).thisPage.sortBy(_.rowId) should be
      ((1 to 10).map { i => FieldValue(dataset.internalName, 1, 4, i, "giraffe " + i) })
    // And the current copy should be bumped to the latest data version
    client.datasetCopy(dataset.internalName, 1).get should be
      (DatasetCopy(dataset.internalName, 1, 4, LifecycleStage.Unpublished))


    ////////////////////////////////////////////////////////////////////////////////////////////////
    // REPLACE ROWS
    // Soda Fountain API: PUT /resource/{resource_name}
    // Resulting secondary events:
    // - Truncated event
    // - LastModifiedChanged event - sent along with any change to the dataset
    ////////////////////////////////////////////////////////////////////////////////////////////////
    val rowReplaceEvents = locally {
      val inserts = (1 to 10).map { i =>
        Insert(new RowId(i), ColumnIdMap[SoQLValue](
          new ColumnId(4) -> SoQLText("axolotl " + i), new ColumnId(5) -> SoQLNumber(new BigDecimal(5))))
      }
      val rowDataUpdated = RowDataUpdated(inserts)

      val lastModifiedChanged = LastModifiedChanged(DateTime.now)

      Iterator(Truncated, rowDataUpdated, lastModifiedChanged)
    }

    // Secondary processes the row replace events
    secondary.version(dataset, 5, None, rowReplaceEvents)

    // We should see 10 different field_value docs in Elastic Search
    client.searchFieldValuesByCopyNumber(dataset.internalName, 1).totalHits should be (10)
    client.searchFieldValuesByCopyNumber(dataset.internalName, 1).thisPage.sortBy(_.rowId) should be
    ((1 to 10).map { i => FieldValue(dataset.internalName, 1, 4, i, "axolotl " + i) })
    // And the current copy should be bumped to the latest data version
    client.datasetCopy(dataset.internalName, 1).get should be
    (DatasetCopy(dataset.internalName, 1, 5, LifecycleStage.Unpublished))

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // PUBLISH
    // Soda Fountain API: PUT /dataset-copy/{resource_name}
    // Resulting secondary events:
    // - WorkingCopyPublished event
    // - RowDataUpdated event
    // - LastModifiedChanged event - sent along with any change to the dataset
    ////////////////////////////////////////////////////////////////////////////////////////////////
    val publishedEvents = locally {
      val lastModifiedChanged = LastModifiedChanged(DateTime.now)
      Iterator(WorkingCopyPublished, lastModifiedChanged)
    }

    // Secondary processes the publish events
    secondary.version(dataset, 6, None, publishedEvents)

    // The current copy should be bumped to the latest data version AND marked as Published.
    client.datasetCopy(dataset.internalName, 1).get should be
      (DatasetCopy(dataset.internalName, 1, 6, LifecycleStage.Published))

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // MAKE A WORKING COPY
    // Soda Fountain API: GET /dataset-copy/{resource_name}?copy_data=true
    // Resulting secondary events:
    // - WorkingCopyCreated event
    // - One ColumnCreated event for every column in the dataset, both system and user columns
    // - DataCopied event to copy over the rows
    // - VersionColumnChanged event designating the default version column on the new copy
    // - SystemRowIdentifierChanged event designating the system row identifier on the new copy
    // - LastModifiedChanged event - sent along with any change to the dataset
    ////////////////////////////////////////////////////////////////////////////////////////////////
    val workingCopyCreatedEvents = locally {
      val workingCopyCreated = WorkingCopyCreated(CopyInfo(
        systemId = new CopyId(297),
        copyNumber = 2,
        lifecycleStage = LifecycleStage.Unpublished,
        dataVersion = 5,
        lastModified = DateTime.now))

      val addUpdatedAtColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(0),
        id = new UserColumnId(":updated_at"),
        fieldName = Some(ColumnName(":updated_at")),
        typ = SoQLFixedTimestamp,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val addVersionColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(1),
        id = new UserColumnId(":version"),
        fieldName = Some(ColumnName(":version")),
        typ = SoQLVersion,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val addIdColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(2),
        id = new UserColumnId(":id"),
        fieldName = Some(ColumnName(":id")),
        typ = SoQLID,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val addCreatedAtColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(3),
        id = new UserColumnId(":created_at"),
        fieldName = Some(ColumnName(":created_at")),
        typ = SoQLFixedTimestamp,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val addTextColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(4),
        id = new UserColumnId("ggha-z6i9"),
        fieldName = Some(ColumnName("test-field")),
        typ = SoQLText,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val addNumberColumn = ColumnCreated(ColumnInfo(
        systemId = new ColumnId(5),
        id = new UserColumnId("j9bp-sh9q"),
        fieldName = Some(ColumnName("numbers")),
        typ = SoQLNumber,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val versionColumnChanged = VersionColumnChanged(ColumnInfo(
        systemId = new ColumnId(1),
        id = new UserColumnId(":version"),
        fieldName = Some(ColumnName(":version")),
        typ = SoQLVersion,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = true,
        computationStrategyInfo = None))

      val systemRowIdChanged = SystemRowIdentifierChanged(ColumnInfo(
        systemId = new ColumnId(2),
        id = new UserColumnId(":id"),
        fieldName = Some(ColumnName(":id")),
        typ = SoQLID,
        isSystemPrimaryKey = true,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val lastModifiedChanged = LastModifiedChanged(DateTime.now)

      Iterator(workingCopyCreated, addUpdatedAtColumn, addVersionColumn, addIdColumn, addCreatedAtColumn,
        addTextColumn, addNumberColumn, DataCopied, versionColumnChanged, systemRowIdChanged, lastModifiedChanged)
    }

    // Secondary processes the gigamundo bundle of events
    secondary.version(dataset, 7, None, workingCopyCreatedEvents)

    // We should now see a brand new dataset_copy doc in Elastic Search
    client.datasetCopy(dataset.internalName, 1) should be
      (DatasetCopy(dataset.internalName, 1, 6, LifecycleStage.Published))
    client.datasetCopy(dataset.internalName, 2).get should be
      (DatasetCopy(dataset.internalName, 2, 7, LifecycleStage.Unpublished))
    // All column maps that exist on copy 1 should now exist on copy 2
    client.columnMap(dataset.internalName, 2, "ggha-z6i9").get should be
      (ColumnMap(dataset.internalName, 2, 4, "ggha-z6i9"))
    // All rows that exist on copy 1 should not exist on copy 2
    client.searchFieldValuesByCopyNumber(dataset.internalName, 2).totalHits should be (10)
    client.searchFieldValuesByCopyNumber(dataset.internalName, 2).thisPage.sortBy(_.rowId) should be
      ((1 to 10).map { i => FieldValue(dataset.internalName, 2, 4, i, "giraffe " + i) })


    ////////////////////////////////////////////////////////////////////////////////////////////////
    // DROP COLUMN
    // Soda Fountain API: DELETE /dataset/{resource_name}/{column_name}
    // Resulting secondary events:
    // - ColumnRemoved event
    // - LastModifiedChanged event - sent along with any change to the dataset
    ////////////////////////////////////////////////////////////////////////////////////////////////
    val columnRemovedEvents = locally {
      val columnRemoved = ColumnRemoved(ColumnInfo(
        systemId = new ColumnId(4),
        id = new UserColumnId("ggha-z6i9"),
        fieldName = Some(ColumnName("text-field")),
        typ = SoQLText,
        isSystemPrimaryKey = false,
        isUserPrimaryKey = false,
        isVersion = false,
        computationStrategyInfo = None))

      val lastModifiedChanged = LastModifiedChanged(DateTime.now)

      Iterator(columnRemoved, lastModifiedChanged)
    }

    // Secondary processes the events
    secondary.version(dataset, 8, None, columnRemovedEvents)

    // The column mapping and any related field values should be removed from the index
    client.columnMap(dataset.internalName, 2, "ggha-z6i9") should not be 'defined
    client.searchFieldValuesByColumnId(dataset.internalName, 2, 4).totalHits should be (0)

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // DROP WORKING COPY
    // Soda Fountain API: DELETE /dataset-copy/{resource_name}
    // Resulting secondary events:
    // - WorkingCopyDropped event
    // - LastModifiedChanged event - sent along with any change to the dataset
    ////////////////////////////////////////////////////////////////////////////////////////////////
    val workingCopyDroppedEvents = locally {
      val lastModifiedChanged = LastModifiedChanged(DateTime.now)
      Iterator(WorkingCopyDropped, lastModifiedChanged)
    }

    // Secondary processes the events
    secondary.version(dataset, 9, None, workingCopyDroppedEvents)

    // All signs of the working copy should be gone from Elastic Search
    // Data version on previous published copy should be updated
    client.datasetCopy(dataset.internalName, 1).get should be
      (DatasetCopy(dataset.internalName, 1, 9, LifecycleStage.Published))
    client.datasetCopy(dataset.internalName, 2) should not be 'defined
    client.searchColumnMapsByCopyNumber(dataset.internalName, 2).totalHits should be (0)
    client.searchFieldValuesByCopyNumber(dataset.internalName, 2).totalHits should be (0)

    // TODO : SnapshotDropped
  }
}
