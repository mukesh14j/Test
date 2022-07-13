package com.ms.ci.hydra.data.pipeline.service

import com.google.gson.JsonObject
import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

class TaskDelegator(taskConfig: Map[String, List[String]], task: Tasks, dataConfig: DataConfig)(implicit spark:SparkSession) extends SparkJobLogging {

  def loadDF(path: Path): DataFrame = {
    var dataframeLoader: DataFrame = null
    taskConfig.getOrElse("loaders", task.loader).map {
      loader =>
        logger.info(s"Running Loader $loader")
        dataframeLoader = dataConfig.loaders(loader).loadDataframe(new Path(""))
    }
    dataframeLoader
  }

  def ruleRunner(ruleDataframe: DataFrame): DataFrame = {
    var rulesDF: DataFrame = ruleDataframe
    taskConfig.getOrElse("ruleEngine", task.ruleEngine).map {
      ruleEngine =>
        logger.info(s"Running RuleEngine $ruleEngine")
        rulesDF = dataConfig.rulesEngine.get(ruleEngine).get.ruleRunner(ruleDataframe)
    }
    rulesDF
  }

  def sinkToDB(path: Path, ruleDataframe: DataFrame): JsonObject = {
    var jsonObject: JsonObject = null
    taskConfig.getOrElse("sinks", task.sinks).map {
      sinks =>
        logger.info(s"Running sink $sinks")
        jsonObject = dataConfig.sinks.get(sinks).get.DBSink(path,ruleDataframe)
    }
    jsonObject
  }

  def reconValidator(dataConf: JsonObject): Unit = {

  }

  def shutdownHooks(): Unit = {

  }

}
