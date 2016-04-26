/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util

import akka.Done
import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.event.LoggingReceive
import akka.kafka.ConsumerSettings
import akka.kafka.internal.ByPartitionActor.RegisterSubSource
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{Future, Promise}

object SubSourceActor {
  case object Complete
}
class SubSourceActor[K, V](mainSource: ActorRef, tp: TopicPartition, kafkaPump: ActorRef) extends ActorPublisher[ConsumerRecord[K, V]] {
  import SubSourceActor._
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
        kafkaPump ! KafkaPump.RequestMessages(List(tp))
      }
    case Cancel => context.stop(self)
    case Complete => onCompleteThenStop()
  }

  override def preStart(): Unit = {
    super.preStart()
    mainSource ! RegisterSubSource(tp)
  }
}

object ByPartitionActor {
  case class RegisterSubSource(tp: TopicPartition)
  private case object Stop
  private case object Stopped

  def apply[K, V](settings: ConsumerSettings[K, V])(implicit as: ActorSystem): Source[(TopicPartition, Source[ConsumerRecord[K, V], Control]), Control] = {
    Source.actorPublisher[(TopicPartition, Source[ConsumerRecord[K, V], Control])](Props(new ByPartitionActor(settings)))
      .mapMaterializedValue { ref =>
        val _shutdown = Promise[Done]
        val _stop = Promise[Done]

        val proxyActor = as.actorOf(Props(new Actor {
          override def receive: Receive = {
            case Terminated(`ref`) => _shutdown.trySuccess(Done)
            case Stopped => _stop.trySuccess(Done)
          }
        }))
        new Control {
          override def shutdown(): Future[Done] = {
            as.stop(ref)
            _shutdown.future
          }
          override def stop(): Future[Done] = {
            ref.tell(Stop, proxyActor)
            _stop.future
          }
          override def isShutdown: Future[Done] = _shutdown.future
        }
      }
  }
}
class ByPartitionActor[K, V](settings: ConsumerSettings[K, V]) extends ActorPublisher[(TopicPartition, Source[ConsumerRecord[K, V], Control])] {
  import ByPartitionActor._
  var kafkaPump: ActorRef = _

  override def preStart(): Unit = {
    super.preStart()
    kafkaPump = context.actorOf(Props(new KafkaPump(settings.createKafkaConsumer)))
    kafkaPump ! KafkaPump.Subscribe(settings.topics.toList)
  }

  override def postStop(): Unit = {
    children.values.foreach(context.stop)
    context.stop(kafkaPump)
    super.postStop()
  }

  override def receive: Receive = normal

  def createSource(tp: TopicPartition): Source[ConsumerRecord[K, V], Control] = {
    Source.actorPublisher(Props(new SubSourceActor(self, tp, kafkaPump)))
      .mapMaterializedValue(ref => new Control {
        override def stop(): Future[Done] = ???
        override def shutdown(): Future[Done] = ???
        override def isShutdown: Future[Done] = ???
      })
  }

  @tailrec
  private def pump(): Unit = {
    if (buffer.nonEmpty && totalDemand > 0) {
      val (tp, remains) = buffer.dequeue
      buffer = remains
      onNext((tp, createSource(tp)))
      pump()
    }
  }

  var buffer: immutable.Queue[TopicPartition] = immutable.Queue.empty
  var children: Map[TopicPartition, ActorRef] = immutable.Map.empty

  def normal: Receive = LoggingReceive {
    case RegisterSubSource(tp) =>
      context.watch(sender)
      children += (tp -> sender)
    case KafkaPump.Assigned(tps) =>
      buffer = buffer.enqueue(tps.to[immutable.Iterable])
      pump()
    case KafkaPump.Revoked(tps) =>
      tps.foreach { tp =>
        children.get(tp) match {
          case Some(ref) =>
            context.unwatch(ref)
            context.stop(ref)
          case None =>
            buffer = buffer.filter(x => tps.contains(x))
        }
      }
    case Terminated(ref) =>
      children.find(_._2 == ref) match {
        case Some((tp, _)) =>
          children -= tp
          buffer :+= tp
          pump()
        case None =>
      }
    case Request(_) => pump()
    case Cancel => context.stop(self)
    case Stop => onComplete()
  }
}

object KafkaPump {
  case class Assigned(partition: List[TopicPartition])
  case class Revoked(partition: List[TopicPartition])
  case class Messages[K, V](messages: Iterator[ConsumerRecord[K, V]])
  case class Subscribe(topics: List[String])
  case class RequestMessages(topics: List[TopicPartition])
  private case object Poll
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
      val rawResult = consumer.poll(pollTimeout().toMillis)

      //push this into separate actor or thread to provide maximum fetching speed
      val result = rawResult.groupBy(x => (x.topic(), x.partition()))
      val (nonEmptyTP, emptyTP) = requests.map {
        case (topics, ref) =>
          val messages = topics.toIterator.flatMap { tp =>
            result.getOrElse((tp.topic(), tp.partition()), Iterator.empty)
          }
          (topics, ref, messages)
      }.partition(_._3.hasNext)
      nonEmptyTP.foreach { case (_, ref, messages) => ref ! Messages(messages) }
      requests = emptyTP.map { case (tp, ref, _) => (tp, ref) }

      if(requests.isEmpty) {
        schedulePoll()
      }
      else {
        self ! Poll
      }
    case RequestMessages(topics) => requests = (topics -> sender) :: requests
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
