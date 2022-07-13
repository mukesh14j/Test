package com.ms.ci.hydra.data.pipeline.utils

import com.ms.ci.hydra.data.pipeline.SparkJobLogging

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

object RetryUtils extends SparkJobLogging{

  def withRetries[T](callback:() =>T, Tries:Integer=3, message:Exception=new Exception("All attempts failed")):Unit={
    val sleepTime = new AtomicLong(500)
    val counter = new AtomicInteger(0)
    while(counter.getAndIncrement()< Tries){
      try {
        return callback()
      }
      catch {
        case ex @(_:Exception | _:Error) =>
          logger.warn(s"Attempt $counter failed")
          logger.warn(s"${ex} : ${ex.getMessage}")
          Thread.sleep(sleepTime.getAndUpdate(_*2))
      }
    }
    throw  message
  }
}
