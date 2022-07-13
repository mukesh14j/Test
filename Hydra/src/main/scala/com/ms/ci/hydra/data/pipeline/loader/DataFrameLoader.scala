package com.ms.ci.hydra.data.pipeline.loader
import org.apache.spark.sql.{DataFrame}
import org.apache.hadoop.fs.Path

trait DataFrameLoader {

  def loadDataframe(path: Path): DataFrame

}
