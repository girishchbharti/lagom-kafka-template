package com.knoldus.api.subscriber

import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.{Done, NotUsed}
import akka.stream.scaladsl.Flow
import com.knoldus.external.{ExternalService, KafkaMessage, KafkaMessageWithMetadata}
import com.lightbend.lagom.scaladsl.api.broker.Message
import com.lightbend.lagom.scaladsl.broker.kafka.KafkaMetadataKeys
import akka.pattern.ask
import akka.remote.WireFormats.FiniteDuration
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class KafkaSubscriber(
                       externalService: ExternalService,
                       actorRef1: ActorRef
                     ) extends FlowHelper {

  override val actorRef: ActorRef = actorRef1

  val consumerGroup = "consumer-group-1"


  implicit val timeout = akka.util.Timeout(5, TimeUnit.SECONDS)

  // Start consuming messages from the kafka topic
  // Where inbound topic is the topic from where we need to consume messages
  // subscribe is used to obtain a subscriber to this topic
  // withGroupId returns A copy of this subscriber with the passed group id
  // withMetadata returns this subscriber, but message payloads are wrapped in [[Message]] instances to allow
  //  --- accessing any metadata associated with the message.
  // atLeastOnce : Applies the passed `flow` to the messages processed by this subscriber. Messages are delivered to the passed
  //   * `flow` at least once.
  externalService.inboundTopic.subscribe.withGroupId(consumerGroup).withMetadata.atLeastOnce {
    kafkaMessageFlow
  }

}

trait FlowHelper{

  implicit val timeOut = Timeout(5000.milli)
  val actorRef: ActorRef
  val parallelism = 8

  val terminateFlow: Flow[Any, Done, NotUsed] = Flow[Any].map(_ => Done)

  val forwardKafkaMessageToWorker: Flow[KafkaMessageWithMetadata, Done, NotUsed] = Flow[KafkaMessageWithMetadata]
    .mapAsync(parallelism) { kafkaMessageWithMeta =>
      (actorRef ? kafkaMessageWithMeta)
        .map(_ => Done)
        .recover {
          case ex: Exception =>
            println("Exception found while waiting for processor response: " + ex)
            Done
        }
    }


  val processKafkaMessgeFlow: Flow[KafkaMessageWithMetadata, Done, NotUsed] = Flow[KafkaMessageWithMetadata]
    .map(kafkaMessageWithMetadata => {
      println(s"Processing kafka message: $kafkaMessageWithMetadata")
      kafkaMessageWithMetadata
    })
    .via(terminateFlow)

  val kafkaMessageFlow: Flow[Message[KafkaMessage], Done, NotUsed] = Flow[Message[KafkaMessage]]
    .map { msg =>
      val messageKey = Try(msg.messageKeyAsString).toOption
      val kafkaHeaders = msg.get(KafkaMetadataKeys.Headers)
      val offset = msg.get(KafkaMetadataKeys.Offset).getOrElse(0L)
      val partition = msg.get(KafkaMetadataKeys.Partition).getOrElse(0)
      val kafkaTimestamp = msg.get(KafkaMetadataKeys.Timestamp)
      val kafkaMessageWithMetadata: KafkaMessageWithMetadata = KafkaMessageWithMetadata(msg.payload, inboundKafkaTimestamp = kafkaTimestamp)
      println(s"Inbound Kafka message arrived: Message: [$kafkaMessageWithMetadata] Key: [$messageKey] Headers: [$kafkaHeaders]," +
        s" partition: [$partition], offset: [$offset], inboundKafkaTimestamp: $kafkaTimestamp at: ${LocalDateTime.now}")
      kafkaMessageWithMetadata
    }
    .via(terminateFlow)
}
