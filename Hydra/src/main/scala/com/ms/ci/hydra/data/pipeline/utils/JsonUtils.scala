package com.ms.ci.hydra.data.pipeline.utils

import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import org.json4s.DefaultFormats
import org.json4s.native.{JsonMethods, Serialization}

import scala.util.Try


object JsonUtils extends  SparkJobLogging{
  implicit  val format = DefaultFormats

  def parse[A:Manifest](json:String, logSuccess:Boolean= false): Try[A] = {
    val res = Try(JsonMethods.parse(json).extract[A])
    if(res.isSuccess  && logSuccess) logger.info(s"parsed json $json")
    res.failed.foreach(ex => logger.info(s"Failed to parsed json due to ${ex.getMessage}: $json"))
    res
  }

  def write(message:AnyRef):String =
    Serialization.write(message)

}
