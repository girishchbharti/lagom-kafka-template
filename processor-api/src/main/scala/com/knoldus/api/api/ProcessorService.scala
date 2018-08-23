package com.knoldus.api.api

import akka.NotUsed
import com.lightbend.lagom.scaladsl.api.{Descriptor, Service, ServiceCall}

trait ProcessorService extends Service{

  def getTopicMessage(topicName: String): ServiceCall[NotUsed, String]

  override def descriptor: Descriptor = {
    import Service._

    named("file-processor").withCalls(
      pathCall("/api/processor/:topic", getTopicMessage _)
    ).withTopics().withAutoAcl(true)
  }
}
