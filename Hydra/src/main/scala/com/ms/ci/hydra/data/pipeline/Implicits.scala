package com.ms.ci.hydra.data.pipeline

import com.google.gson.Gson
import com.typesafe.config.Config
import org.apache.avro.Schema
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.io.Source


object Implicits {

  implicit class EnhancedTypeSafeConfig(config:Config){

    def getMap(path:String):Map[String,String] ={
      config.getConfig(path).asMap
    }

    def asMap:Map[String,String] ={
      config.entrySet().asScala.map {entry =>
        entry.getKey -> entry.getValue.unwrapped().toString
      }.toMap
    }

    def getSeq(path:String):Seq[String] =
      config.getStringList(path).asScala.toList

    def parse[A: Manifest](json: String,printSuccess:Boolean = false):Schema={
      val gson = new Gson()
      gson.fromJson(json, classOf[Schema])
    }

    def readContentFromPath(path:Path, fs:FileSystem):String ={
      Source.fromInputStream(fs.open(path)).mkString
    }

  }

}
