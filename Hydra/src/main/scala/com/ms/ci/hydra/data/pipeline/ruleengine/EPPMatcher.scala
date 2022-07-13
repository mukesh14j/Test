package com.ms.ci.hydra.data.pipeline.ruleengine

import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

class EPPMatcher(params: Map[String,String]) extends RuleEngine with SparkJobLogging{

  override def ruleRunner(dataFrame: DataFrame) (implicit spark: SparkSession): DataFrame = ???
}
