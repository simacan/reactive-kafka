/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import akka.actor.{ActorSystem, Props}
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer.CommittableMessage
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord

object ActorAPI {
  class PlainSourceActor[K, V](settings: ConsumerSettings[K, V])
    extends SourceActor[K, V, ConsumerRecord[K, V]](settings)
    with PlainMessageBuilder[K, V]

  class CommittableSourceActor[K, V](settings: ConsumerSettings[K, V])
    extends SourceActor[K, V, CommittableMessage[K, V]](settings)
    with CommittableMessageBuilder[K, V]
}
