package com.ms.ci.hydra.data.pipeline
import java.time.Instant
import java.util.concurrent.TimeUnit


object TimeLogger extends SparkJobLogging {


  def apply[T](runnable: =>T): Long = {
      val start = Instant.now().toEpochMilli
      val result = runnable
      val end = Instant.now().toEpochMilli
      durationInSec(start,end)
  }

  def Time[T](runnable: =>T): T = {
    val start = Instant.now().toEpochMilli
    val result = runnable
    val end = Instant.now().toEpochMilli
    logger.info(s"time taken to execute Method is: ${durationInSec(start,end)} secs")
    result
  }



  def durationInSec(start: Long, end: Long, mins: Boolean=false): Long = {
  if(start <0 && end< 0){
    throw new IllegalArgumentException(s"start: $start end: $end")
  }
    val dif = end -start
    val minutes = mins match {
      case false => TimeUnit.MILLISECONDS.toSeconds(dif)
      case true => TimeUnit.MILLISECONDS.toMinutes(dif)
    }
    minutes
  }

}
