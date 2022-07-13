package com.ms.ci.hydra.data.pipeline.preprocessor

import org.apache.spark.sql.SparkSession

trait PreProcessor[T] {
  def run() (implicit spark: SparkSession)

}
