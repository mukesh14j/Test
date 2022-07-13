package com.ms.ci.hydra.data.pipeline.utils
import com.ms.ci.hydra.data.pipeline.SparkJobLogging
import java.util.Properties
import javax.mail.{Message, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage}
import scala.util.Try

class MailSender(smtpParams:Map[String,String], from:String, to:String) extends SparkJobLogging with  Serializable {

  private def builderProperties:Properties = {
    val props = new Properties()
    smtpParams.foreach(tuple => props.put(tuple._1, tuple._2))
    props
  }

  def send(subject:String,body:String):Unit ={
    val props = builderProperties
    logger.info(s"creating java mail  mime message with parameters $props")
    val message = new MimeMessage(Session.getDefaultInstance(props, null))

    message.setRecipients(Message.RecipientType.TO, to)
    message.setSubject(subject)
    message.setFrom(new InternetAddress(from))

    val bodyPart = new MimeBodyPart()
    message.setContent(body, "test/html")
    logger.info(s"Attempting to send message from=$from to=$to subject=$subject bosy=$body")
    val t = Try {
    RetryUtils.withRetries(() => Transport.send(message))
    }
    if(t.isFailure){
      logger.info(s"email attempt failed after 3 attempts not trying")
    }

  }

}
