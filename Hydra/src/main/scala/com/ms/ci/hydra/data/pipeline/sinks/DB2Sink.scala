package com.ms.ci.hydra.data.pipeline.sinks

import com.google.gson.JsonObject
import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

class DB2Sink(params: Map[String,String]) extends DataframeSink with  SparkJobLogging {
  override def DBSink(path: Path, df: DataFrame) (implicit spark: SparkSession): JsonObject = {

     null
  }
}
