package com.knoldus.api.impl

import akka.NotUsed
import com.knoldus.api.api.ProcessorService
import com.lightbend.lagom.scaladsl.api.ServiceCall
import com.lightbend.lagom.scaladsl.server.ServerServiceCall

import scala.concurrent.Future

class ProcessorServiceImpl extends ProcessorService{

  override def getTopicMessage(topicName: String): ServiceCall[NotUsed, String] = ServerServiceCall { _ =>
    Future.successful("data...")
  }

}
