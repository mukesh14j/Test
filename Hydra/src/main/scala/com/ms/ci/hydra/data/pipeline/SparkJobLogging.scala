package com.ms.ci.hydra.data.pipeline

import org.slf4j.{Logger, LoggerFactory}

trait SparkJobLogging {

  @transient protected  lazy  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

}
