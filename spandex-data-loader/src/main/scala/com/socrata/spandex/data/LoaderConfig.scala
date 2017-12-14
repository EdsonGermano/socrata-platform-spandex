package com.socrata.spandex.data

import java.io.File

private[data] case class LoaderConfig(
    dataFile: File = new File("."),
    datasetId: String = "",
    columns: List[String] = Nil)
