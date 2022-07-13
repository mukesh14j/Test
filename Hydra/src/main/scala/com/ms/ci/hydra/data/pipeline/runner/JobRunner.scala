package com.ms.ci.hydra.data.pipeline.runner

import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import com.ms.ci.hydra.data.pipeline.service.{DataConfig, DataFlowPipeline}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import java.io.File


final case class Request(tasks:Map[String,Map[String, List[String]]]) extends AnyRef with Product with Serializable

class JobRunner(args:Array[String]) extends Runner with SparkJobLogging with Serializable {

  def addTask(head: String): Request = {
    Request(Map(head -> Map()))
  }

  override def execute(): Unit = {
    val request:Request = addTask(args.head)
    execCommonFlow(request)
  }

  private def execCommonFlow(request: Request):Unit ={
    val conf = getDataConfig()
    logger.info(s"conf is $conf")
    implicit  val dataConfiguration:DataConfig = getDataConfig(conf)
    logger.info(s"loaded Data Configuration  $dataConfiguration")

    implicit val builder = createSparkBuilder(dataConfiguration.additionalSparkConf)
    val dataFlowPipeline = new DataFlowPipeline(request)
    dataFlowPipeline.exec(dataFlowPipeline)
  }

  def getDataConfig() :Config ={
    val file  = new File(System.getenv("config.file"))
    logger.info(s"loaded File ${file.exists()}")
    val confFile = ConfigFactory.parseFile(file)
    val confFile2 = ConfigFactory.parseFile(new File("processList.conf"))
    return ConfigFactory.load().withFallback(confFile).withFallback(confFile2).resolve()
  }

  private def getDataConfig(config: Config):DataConfig ={
    DataFlowPipeline.loadFromConfig(config)
  }

  def createSparkBuilder(additionalSparkConf: Map[String, String]):SparkSession.Builder ={
    val sparkBuilder = SparkSession.builder().enableHiveSupport()
    val builder = additionalSparkConf.foldLeft(sparkBuilder){ case (b, (param,value)) =>
      logger.info(s"changing parameter $param for spark")
      b.config(param, value)
    }
    builder
  }

//  def getJsonObj[T](jsonString:String)(implicit m:Manifest[T]) :T = {
//    extractFrom(jsonString) match {
//      case Success(parsedJson) =>
//        parsedJson
//      case Failure(ex) =>
//        throw new IllegalArgumentException(ex)
//    }
//  }
//
//  private def extractFrom[T] (jsonString:String)(implicit m:Manifest[T]) : Try[T] = {
//    lazy implicit val format = new DefaultFormats {
//      override val typeHintFieldname: String = "eventType"
//      override val typeHints: TypeHints = ShortTypeHints(List(classOf[]))
//    }
//  }
}
