package com.ms.ci.hydra.data.pipeline.sinks

import com.google.gson.JsonObject
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataframeSink {

  def DBSink(path:Path, df:DataFrame)(implicit spark: SparkSession):JsonObject
}
