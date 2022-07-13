package com.ms.ci.hydra.data.pipeline.ruleengine

import org.apache.spark.sql.{DataFrame, SparkSession}

trait RuleEngine {

  def ruleRunner(dataFrame: DataFrame) (implicit spark: SparkSession): DataFrame
}
