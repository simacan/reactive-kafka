package akka.kafka.internal

import akka.actor.{ActorSystem, Props}
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer.CommittableMessage
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord

object ActorAPI {
  private class PlainSourceActor[K, V](settings: ConsumerSettings[K, V])
    extends SourceActor[K, V, ConsumerRecord[K, V]](settings)
    with PlainMessageBuilder[K, V]
  private class CommittableSourceActor[K, V](settings: ConsumerSettings[K, V])
    extends SourceActor[K, V, CommittableMessage[K, V]](settings)
    with CommittableMessageBuilder[K, V]

  private def fromActor[T](ref: => ActorPublisher[T]) = {
    Source.actorPublisher[T](Props(ref))
      .mapMaterializedValue(ActorControl(_))

  }

  def plain[K, V](settings: ConsumerSettings[K, V])(implicit as: ActorSystem) = {
    fromActor(new PlainSourceActor[K, V](settings))
  }

  def committable[K, V](settings: ConsumerSettings[K, V])(implicit as: ActorSystem) = {
    fromActor(new CommittableSourceActor[K, V](settings))
  }
}