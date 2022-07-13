package com.ms.ci.hydra.data.pipeline.runner

trait Runner {

  @throws(classOf[Exception])
  def execute()

}
