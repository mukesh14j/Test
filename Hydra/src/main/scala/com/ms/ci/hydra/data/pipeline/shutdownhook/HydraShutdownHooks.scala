package com.ms.ci.hydra.data.pipeline.shutdownhook

import com.ms.ci.hydra.data.pipeline.SparkJobLogging

class HydraShutdownHooks(params: Map[String,String]) extends ShutDownHooks with SparkJobLogging{

}
