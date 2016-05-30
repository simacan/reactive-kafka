/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.scaladsl

import java.util

import akka.Done
import akka.actor.{ActorRef, ActorRefFactory}
import akka.kafka.ConsumerSettings
import akka.kafka.internal.KafkaConsumerActor
import akka.util.Timeout
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

object KafkaAsyncConsumer {
  def apply[K, V](settings: ConsumerSettings[K, V])(implicit as: ActorRefFactory): KafkaAsyncConsumer[K, V] = new KafkaAsyncConsumer[K, V] {
    import KafkaConsumerActor._
    import akka.pattern.ask
    val ref = as.actorOf(KafkaConsumerActor.props(settings))
    val finished = {
      implicit val to = Timeout(settings.closeTimeout)
      Await.result((ref ? Watch).mapTo[Future[Done]], settings.closeTimeout)
    }

    override def subscribe(topics: Set[String], listener: ConsumerRebalanceListener): Unit = {
      ref ! Subscribe(topics, listener)
    }

    override def subscribe(pattern: String, listener: ConsumerRebalanceListener): Unit = {
      ref ! SubscribePattern(pattern, listener)
    }

    override def close(): Future[Done] = {
      ref ! Stop
      finished
    }

    override def request(tps: Set[TopicPartition])(implicit ec: ExecutionContext): Future[Iterator[ConsumerRecord[K, V]]] = {
      implicit val to = Timeout(1000 hours)
      (ref ? RequestMessages(tps)).mapTo[Messages[K, V]].map(_.messages)
    }

    override def commit(offsets: Map[TopicPartition, Long])(implicit ec: ExecutionContext): Future[Map[TopicPartition, OffsetAndMetadata]] = {
      implicit val to = Timeout(settings.commitTimeout)
      (ref ? Commit(offsets)).mapTo[Committed].map(_.offsets)
    }

    override def assign(tps: Set[TopicPartition]): Unit = {
      ref ! Assign(tps)
    }

    override def assign(tps: Map[TopicPartition, Long]): Unit = {
      ref ! AssignWithOffset(tps)
    }
  }

  private[kafka] def rebalanceListener(onAssign: Iterable[TopicPartition] => Unit, onRevoke: Iterable[TopicPartition] => Unit) = new ConsumerRebalanceListener {
    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
      onAssign(partitions.asScala)
    }
    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      onRevoke(partitions.asScala)
    }
  }
}

trait KafkaAsyncConsumer[K, V] {
  def ref: ActorRef
  def subscribe(topics: Set[String], listener: ConsumerRebalanceListener): Unit
  def subscribe(pattern: String, listener: ConsumerRebalanceListener): Unit
  def assign(tps: Set[TopicPartition]): Unit
  def assign(tps: Map[TopicPartition, Long]): Unit
  def request(tps: Set[TopicPartition])(implicit ec: ExecutionContext): Future[Iterator[ConsumerRecord[K, V]]]
  def commit(offsets: Map[TopicPartition, Long])(implicit ec: ExecutionContext): Future[Map[TopicPartition, OffsetAndMetadata]]
  def close(): Future[Done]
}
