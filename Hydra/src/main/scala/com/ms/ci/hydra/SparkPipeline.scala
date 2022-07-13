package com.ms.ci.hydra

import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import com.ms.ci.hydra.data.pipeline.runner.JobRunner
import org.apache.log4j.PropertyConfigurator

object SparkPipeline extends App with SparkJobLogging {
  var status:Integer =0
  try {
    PropertyConfigurator.configure(System.getProperty("log4j.configuration"))
    logger.info(s"config.file -> ${System.getenv("config.file")}")
    val runner:JobRunner = new JobRunner(args)
    runner.execute()
  } catch {
    case e: Throwable =>
      logger.error(e.getMessage, e)
  } finally {
    logger.info("exiting now")
  }

}
