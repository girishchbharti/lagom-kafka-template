package com.knoldus.api.app

import akka.actor.ActorRef
import com.knoldus.api.api.ProcessorService
import com.knoldus.api.impl.{ProcessorActor, ProcessorServiceImpl, SerializationRegistry}
import com.knoldus.api.subscriber.KafkaSubscriber
import com.knoldus.external.ExternalService
import com.lightbend.lagom.scaladsl.server._
import com.lightbend.lagom.scaladsl.api.{Descriptor, ServiceLocator}
import com.lightbend.lagom.scaladsl.broker.kafka.LagomKafkaComponents
import com.lightbend.lagom.scaladsl.devmode.LagomDevModeComponents
import com.lightbend.lagom.scaladsl.persistence.cassandra.CassandraPersistenceComponents
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import com.lightbend.lagom.scaladsl.server._
import com.lightbend.lagom.spi.persistence.OffsetStore
import play.api.libs.ws.ahc.AhcWSComponents
import com.softwaremill.macwire.wire
import com.typesafe.conductr.bundlelib.lagom.scaladsl.ConductRApplicationComponents

import scala.concurrent.ExecutionContext


trait ProcessorApplicationComponents extends LagomServerComponents with CassandraPersistenceComponents{

  implicit def executionContext: ExecutionContext

  override lazy val lagomServer = serverFor[ProcessorService](wire[ProcessorServiceImpl])

  lazy val jsonSerializerRegistry: JsonSerializerRegistry = SerializationRegistry

}

abstract class LagomProcessorApplication(context: LagomApplicationContext)
  extends LagomApplication(context)
  with ProcessorApplicationComponents
  with AhcWSComponents
  with LagomKafkaComponents{

  lazy val nimbleExternalService: ExternalService = serviceClient.implement[ExternalService]

  lazy val processorActorRef: ActorRef = ProcessorActor(actorSystem)

  wire[KafkaSubscriber]
}

class ProcessorApplication extends LagomApplicationLoader{
  override def load(context: LagomApplicationContext): LagomApplication =
    new LagomProcessorApplication(context) with ConductRApplicationComponents{
      override lazy val circuitBreakerMetricsProvider = new com.lightbend.lagom.internal.client.CircuitBreakerMetricsProviderImpl(actorSystem)
  }

  override def loadDevMode(context: LagomApplicationContext): LagomApplication = {
    new LagomProcessorApplication(context) with LagomDevModeComponents
  }

  override def describeService: Option[Descriptor] = Some(
    readDescriptor[ProcessorService]
  )
}
