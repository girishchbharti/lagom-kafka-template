package com.knoldus.external

import java.time.Instant

import play.api.libs.json.{Format, Json}

case class KafkaMessage (
                        name: String,
                        date: String,
                        message: String
                        )

object  KafkaMessage{

  implicit val kafkaMessageFormat: Format[KafkaMessage] = Json.format

}

case class KafkaMessageWithMetadata(
                                   kafkaMessage: KafkaMessage,
                                   inboundKafkaTimestamp: Option[Long]
                                   )
