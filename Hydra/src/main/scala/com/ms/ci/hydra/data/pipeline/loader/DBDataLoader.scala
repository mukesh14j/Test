package com.ms.ci.hydra.data.pipeline.loader
import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

class DBDataLoader(params: Map[String,String]) extends  DataFrameLoader with  SparkJobLogging {

  override def loadDataframe(path: Path): DataFrame = {

    null
  }
}
