package com.ms.ci.hydra.data.pipeline.preprocessor

import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import org.apache.spark.sql.SparkSession

class HydraPreProcessor(params: Map[String,String]) extends PreProcessor[Unit] with SparkJobLogging{

  override def run()(implicit spark: SparkSession): Unit = {
    logger.info(s"Running HydraPreProcessor with params $params")

  }


}
