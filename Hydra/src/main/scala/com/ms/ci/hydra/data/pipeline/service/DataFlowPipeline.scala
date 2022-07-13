package com.ms.ci.hydra.data.pipeline.service

import com.ms.ci.hydra.data.pipeline.Implicits.EnhancedTypeSafeConfig
import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import com.ms.ci.hydra.data.pipeline.loader.DataFrameLoader
import com.ms.ci.hydra.data.pipeline.postprocessor.PostProcessor
import com.ms.ci.hydra.data.pipeline.preprocessor.PreProcessor
import com.ms.ci.hydra.data.pipeline.ruleengine.RuleEngine
import com.ms.ci.hydra.data.pipeline.runner.Request
import com.ms.ci.hydra.data.pipeline.shutdownhook.ShutDownHooks
import com.ms.ci.hydra.data.pipeline.sinks.DataframeSink
import com.ms.ci.hydra.data.pipeline.utils.MailSender
import com.ms.ci.hydra.data.pipeline.validator.Validator
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.util.{Failure, Success, Try}

final case class DataConfig(additionalSparkConf: Map[String,String],preprocessors:Map[String,PreProcessor[Unit]],
                            rulesEngine:Map[String,RuleEngine], validators:Map[String,Validator[Unit]],
                            loaders:Map[String,DataFrameLoader], sinks:Map[String,DataframeSink],
                            postprocessors:Map[String,PostProcessor],shutdownHooks: Map[String,ShutDownHooks],
                            tasks:Map[String,Tasks],mailSend:MailSender)

case class  Tasks(name:String,preprocessors:Seq[String],loader:Seq[String],ruleEngine:Seq[String], sinks:Seq[String],
                  postValidators:Seq[String],postProcessor:Seq[String],shutdownHooks:Seq[String])

class DataFlowPipeline(request: Request)(implicit builder:SparkSession.Builder, implicit  val dataConfig: DataConfig) extends  SparkJobLogging {

  implicit var spark: SparkSession = null

  def execute(): Unit = {
    try{
      request.tasks.map {
        case (taskname: String, taskConfig: Map[String, List[String]]) =>
          logger.info(s"processing task $taskname")
          val task: Tasks = dataConfig.tasks.get(taskname).get

          //Execute PreProcessor
          taskConfig.getOrElse("pre-processors", task.preprocessors).map {
            preProcessor =>
              logger.info(s"Running Preprocessor  $preProcessor")
              if (spark == null) dataConfig.preprocessors.get(preProcessor).get.run()
              logger.info(s"Spark setup done")
          }

          //Create SparkSession
          spark = builder.enableHiveSupport().getOrCreate()
          logger.info(s"Spark initialized $spark")

          val taskDelegator = new TaskDelegator(taskConfig, task, dataConfig)
          Try {
            logger.info(s"Running Loader")
            val rulesDataFrame = taskDelegator.loadDF(new Path(""))
            logger.info(s"Running Rule engine")
            val ruleDF = taskDelegator.ruleRunner(rulesDataFrame)
            logger.info(s"Running Sinks")
            val dataConf = taskDelegator.sinkToDB(new Path(""), ruleDF)
            logger.info(s"Running PostProcessor")
            val postProcessor = taskDelegator.reconValidator(dataConf)

          }.failed.foreach { e =>
            logger.error(s"Error Processing ${e.getMessage}")
          }
          taskDelegator.shutdownHooks()
        case _ =>
      }}
        catch
        {
          case e: Throwable =>
            logger.error(s"some Error occurred ${e.getMessage} , printing stack ${e.getStackTrace}")
            throw e
        }
        finally
        {
          if (spark != null) {
            logger.info("closing spark session")
            spark.stop()
          }
        }
  }

  def exec(dataFlowPipeline: DataFlowPipeline): Unit = {
    dataFlowPipeline.execute()
  }

}
object DataFlowPipeline extends SparkJobLogging {

    def loadFromConfig(rawConfig: Config): DataConfig = {
      val additionalSparkConfig = rawConfig.getMap("additionalSparkConf")
      val mailSender = new MailSender(rawConfig.getMap("mail.smtp"), rawConfig.getString("mail.from"), rawConfig.getString("mail.to"))

      //build PreProcessor Map
      val preProcessor: Map[String, PreProcessor[Unit]] = load[PreProcessor[Unit]](rawConfig, "pre-processors")

      //build Validator Map
      val validator: Map[String, Validator[Unit]] = load[Validator[Unit]](rawConfig, "post-validators")

      //build loader Map
      val loader: Map[String, DataFrameLoader] = load[DataFrameLoader](rawConfig, "loaders")
      require(loader.nonEmpty, "At least one DataLoader is Required")

      //build RuleEngine Map
      val ruleEngine: Map[String, RuleEngine] = load[RuleEngine](rawConfig, "rule-engine")
      require(ruleEngine.nonEmpty, "At least one RuleEngine is Required")

      //Load Sinks
      val sinks: Map[String, DataframeSink] = load[DataframeSink](rawConfig, "sinks")
      require(sinks.nonEmpty, "At least one sinks is Required")

      //build Postprocessor Map
      val Postprocessor: Map[String, PostProcessor] = load[PostProcessor](rawConfig, "post-processors")

      //build shutdownHooks Map
      val shutDownHooks: Map[String, ShutDownHooks] = load[ShutDownHooks](rawConfig, "shutdown-hooks")

      //Build the taskList
      val tasks = rawConfig.getConfigList("tasks").asScala.map { taskConfig =>
        val name = taskConfig.getString("name")
        val preProcessor = taskConfig.getSeq("pre-processors")
        val loader = taskConfig.getSeq("loaders")
        val ruleengine = taskConfig.getSeq("rule-engine")
        val sinks = taskConfig.getSeq("sinks")
        val postValidator = taskConfig.getSeq("post-validators")
        val postProcessors = taskConfig.getSeq("post-processors")
        val shutDownHooks = taskConfig.getSeq("shutdown-hooks")
        name -> Tasks(name, preProcessor, loader, ruleengine, sinks, postValidator, postProcessors, shutDownHooks)
      }.toMap
      DataConfig(additionalSparkConfig, preProcessor, ruleEngine, validator, loader, sinks, Postprocessor, shutDownHooks, tasks, mailSender)
    }

    def load[T](config: Config, entityName: String): Map[String, T] = {
      val entities = config.getConfigList(entityName).asScala.map { entityConfig =>
        val name = entityConfig.getString("name")
        val clazz = entityConfig.getString("type")
        val params = entityConfig.getMap("params")
        val entity = Try {
          val cls = Class.forName(clazz)
          val inst = Try(cls.getConstructor(classOf[Map[String, String]]).newInstance(params))
            .getOrElse(cls.getConstructor(classOf[String], classOf[Map[String, String]])
              .newInstance(name, params))
          inst.asInstanceOf[T]
        } match {
          case Success(retriever) =>
            logger.info(s"loaded entity: ${entityName} of type ${clazz}")
            retriever
          case Failure(ex) =>
            logger.info(s"failed for environment prepare $ex")
            throw ex
        }
        name -> entity
      }.toMap
      entities
    }
}