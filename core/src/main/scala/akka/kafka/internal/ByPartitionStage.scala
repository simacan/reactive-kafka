/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.{immutable, _}
import scala.concurrent.Future

class SubSourceActor[K, V](topic: TopicPartition, kafkaPump: ActorRef) extends ActorPublisher[ConsumerRecord[K, V]] {
  var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
  override def receive: Receive = normal

  @tailrec
  private def pump(): Unit = {
    if (buffer.hasNext && totalDemand > 0) {
      onNext(buffer.next())
      pump()
    }
  }
  def normal: Receive = LoggingReceive {
    case KafkaPump.Messages(msgs) =>
      val newMessages = msgs.asInstanceOf[Iterator[ConsumerRecord[K, V]]]
      buffer = buffer ++ newMessages
      pump()
    case Request(_) =>
      pump()
      if (!buffer.hasNext && totalDemand > 0) {
        kafkaPump ! KafkaPump.Fetch(List(topic))
      }
    case Cancel => context.stop(self)
    case _ =>
  }
}

class ByPartitionActor[K, V](settings: ConsumerSettings[K, V]) extends ActorPublisher[(TopicPartition, Source[ConsumerRecord[K, V], Control])] {
  var kafkaPump: ActorRef = _

  override def preStart(): Unit = {
    super.preStart()
    kafkaPump = context.actorOf(Props(new KafkaPump(settings.createKafkaConsumer)))
    kafkaPump ! KafkaPump.Subscribe(settings.topics.toList)
  }

  override def receive: Receive = normal(immutable.Queue.empty)

  def createSource(tp: TopicPartition): Source[ConsumerRecord[K, V], Control] = {
    Source.actorPublisher(Props(new SubSourceActor(tp, kafkaPump)))
      .mapMaterializedValue(ref => new Control {
        override def stop(): Future[Done] = ???
        override def shutdown(): Future[Done] = ???
        override def isShutdown: Future[Done] = ???
      })
  }

  @tailrec
  private def pump(buffer: immutable.Queue[TopicPartition]): immutable.Queue[TopicPartition] = {
    if (buffer.nonEmpty && totalDemand > 0) {
      val (tp, remains) = buffer.dequeue
      onNext((tp, createSource(tp)))
      pump(remains)
    }
    else {
      buffer
    }
  }

  def normal(buffer: immutable.Queue[TopicPartition]): Receive = LoggingReceive {
    case KafkaPump.Assigned(tps) =>
      val newBuffer = buffer.enqueue(tps.to[immutable.Iterable])
      context.become(
        normal(pump(newBuffer))
      )
    case KafkaPump.Revoked(tps) => context.become(
      normal(buffer.filter(x => tps.contains(x)))
    )
    case Request(_) => context.become(
      normal(pump(buffer))
    )
    case Cancel => context.stop(self)
    case x => println(s"Got unhandled message $x")
  }
}

object KafkaPump {
  case class Assigned(partition: List[TopicPartition])
  case class Revoked(partition: List[TopicPartition])
  case class Messages[K, V](messages: Iterator[ConsumerRecord[K, V]])
  case class Subscribe(topics: List[String])
  case class Fetch(topics: List[TopicPartition])
  private object Poll
}

class KafkaPump[K, V](consumerFactory: () => KafkaConsumer[K, V]) extends Actor {

  import KafkaPump._

  import scala.collection.JavaConversions._
  import scala.concurrent.duration._

  def pollTimeout() = 100.millis

  def pollDelay() = 100.millis

  private var requests = List.empty[(List[TopicPartition], ActorRef)]
  var consumer: KafkaConsumer[K, V] = _

  def receive: Receive = LoggingReceive {
    case Subscribe(topics) =>
      val reply = sender
      consumer.subscribe(topics, new ConsumerRebalanceListener {
        override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
          reply ! Assigned(partitions.toList)
        }

        override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
          reply ! Revoked(partitions.toList)
        }
      })
    case Poll =>
      val partitionsToFetch = requests.flatMap(_._1).toSet
      consumer.assignment().foreach { tp =>
        if (partitionsToFetch.contains(tp)) consumer.resume(tp)
        else consumer.pause(tp)
      }

      val result = consumer
        .poll(pollTimeout().toMillis)
        .groupBy(x => (x.topic(), x.partition()))

      val (nonEmptyTP, emptyTP) = requests.map {
        case (topics, ref) =>
          val messages = topics.toIterator.flatMap { tp =>
            result.getOrElse((tp.topic(), tp.partition()), Iterator.empty)
          }
          (topics, ref, messages)
      }.partition(_._3.hasNext)

      nonEmptyTP.foreach { case (_, ref, messages) => ref ! Messages(messages) }
      requests = emptyTP.map { case (tp, ref, _) => (tp, ref) }

      schedulePoll()
    case Fetch(topics) => requests = (topics -> sender) :: requests
  }

  override def preStart(): Unit = {
    super.preStart()
    consumer = consumerFactory()
    schedulePoll()
  }

  override def postStop(): Unit = {
    consumer.close()
    super.postStop()
  }

  def schedulePoll(): Unit = {
    import context.dispatcher
    context.system.scheduler.scheduleOnce(pollDelay(), self, Poll)
  }
}
