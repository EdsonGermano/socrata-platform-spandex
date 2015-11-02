package com.socrata.spandex.secondary

import com.typesafe.scalalogging.slf4j.Logging
import com.socrata.datacoordinator.secondary.ColumnInfo

trait SecondaryEventLogger extends Logging {
  private[this] def logEvent(eventName: String, description: String): Unit =
    logger.info(s"$eventName event: $description")

  def logDataVersionBump(dataset:String, copyNumber: Long, oldVersion: Long, newVersion: Long): Unit =
    logger.info(s"Bumping data version to $newVersion for dataset $dataset copy $copyNumber")

  def logResync(dataset: String, copyNumber: Long): Unit =
    logEvent("Resync", s"Resyncing dataset $dataset copy $copyNumber")

  def logWorkingCopyCreated(dataset: String, copyNumber: Long): Unit =
    logEvent("WorkingCopyCreated",
             s"registering new copy number $copyNumber for dataset $dataset")

  def logDataCopied(dataset: String, fromCopyNumber: Long, toCopyNumber: Long): Unit =
    logEvent("DataCopied",
      s"Copying data from dataset $dataset copy $fromCopyNumber to copy $toCopyNumber")

  def logColumnCreated(dataset: String, copyNumber: Long, info: ColumnInfo[_]): Unit =
    logEvent("ColumnCreated",
      s"adding column ${info.id.underlying}/${info.systemId.underlying} " +
        s"to column map for dataset $dataset copy $copyNumber")

  def logColumnRemoved(dataset: String, copyNumber: Long, column: String): Unit =
    logEvent("ColumnRemoved",
             s"removing column $column from dataset $dataset copy $copyNumber")

  def logTruncate(dataset: String, copyNumber: Long): Unit =
    logEvent("Truncate",
             s"deleting field values for latest copy $copyNumber of dataset $dataset")

  def logWorkingCopyPublished(dataset: String, copyNumber: Long): Unit =
    logEvent("WorkingCopyPublished",
             s"publishing working copy $copyNumber of dataset $dataset")

  def logWorkingCopyDropped(dataset: String, copyNumber: Long): Unit =
    logEvent("WorkingCopyDropped",
             s"dropped working copy $copyNumber of dataset $dataset")

  def logSnapshotDropped(dataset: String, copyNumber: Long): Unit =
    logEvent("SnapshotDropped",
      s"dropped snapshot copy $copyNumber of dataset $dataset")
}
