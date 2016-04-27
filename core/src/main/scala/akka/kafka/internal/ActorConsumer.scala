/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props, Status, Terminated}
import akka.event.LoggingReceive
import akka.kafka.ConsumerSettings
import akka.kafka.internal.TopicPartitionSourceActor.RegisterSubSource
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{Future, Promise}

object SourceActor {
  def apply[K, V](settings: ConsumerSettings[K, V])(implicit as: ActorSystem): Source[ConsumerRecord[K, V], Control] = {
    Source.actorPublisher[ConsumerRecord[K, V]](Props(new SourceActor(settings)))
      .mapMaterializedValue(ActorControl(_))

  }
}

class SourceActor[K, V](settings: ConsumerSettings[K, V]) extends ActorPublisher[ConsumerRecord[K, V]] {
  var kafka: ActorRef = _
  var tps = Set.empty[TopicPartition]
  var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

  override def preStart(): Unit = {
    super.preStart()
    kafka = context.actorOf(Props(new KafkaActor(settings.createKafkaConsumer)))
    kafka ! KafkaActor.Subscribe(settings.topics)
  }

  @tailrec
  private def pump(): Unit = {
    if (totalDemand > 0) {
      if (!buffer.hasNext) {
        kafka ! KafkaActor.RequestMessages(tps)
      }
      else {
        val msg = buffer.next()
        onNext(msg)
        pump()
      }
    }
  }

  override def receive: Receive = working

  def working: Receive = LoggingReceive {
    case KafkaActor.Assigned(newTps) =>
      tps ++= newTps
      pump()
    case KafkaActor.Revoked(newTps) =>
      tps --= newTps
      pump()
    case KafkaActor.Messages(msgs) =>
      buffer = buffer ++ msgs.asInstanceOf[Iterator[ConsumerRecord[K, V]]]
      pump()
    case Request(_) =>
      pump()
    case ActorControl.Stop =>
      onComplete()
      context.become(stopped)
      sender() ! ActorControl.Stopped
    case Cancel => context.stop(self)
  }

  def stopped: Receive = LoggingReceive {
    case KafkaActor.Assigned(newTps) =>
    case KafkaActor.Revoked(newTps) =>
    case KafkaActor.Messages(msgs) =>
    case ActorControl.Stop => sender() ! ActorControl.Stopped
    case Cancel => context.stop(self)
  }

}

class TopicPartitionSubSourceActor[K, V](mainSource: ActorRef, tp: TopicPartition, kafka: ActorRef)
    extends ActorPublisher[ConsumerRecord[K, V]] {
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
    case KafkaActor.Messages(msgs) =>
      val newMessages = msgs.asInstanceOf[Iterator[ConsumerRecord[K, V]]]
      buffer = buffer ++ newMessages
      pump()
    case Request(_) =>
      pump()
      if (!buffer.hasNext && totalDemand > 0) {
        kafka ! KafkaActor.RequestMessages(Set(tp))
      }
    case Cancel => context.stop(self)
    case ActorControl.Stop => onComplete()
  }

  override def preStart(): Unit = {
    super.preStart()
    mainSource ! RegisterSubSource(tp)
  }
}

object TopicPartitionSourceActor {
  case class RegisterSubSource(tp: TopicPartition)

  def apply[K, V](settings: ConsumerSettings[K, V])(implicit as: ActorSystem): Source[(TopicPartition, Source[ConsumerRecord[K, V], Control]), Control] = {
    Source.actorPublisher[(TopicPartition, Source[ConsumerRecord[K, V], Control])](Props(new TopicPartitionSourceActor(settings)))
      .mapMaterializedValue(ActorControl(_))
  }
}
class TopicPartitionSourceActor[K, V](settings: ConsumerSettings[K, V])
    extends ActorPublisher[(TopicPartition, Source[ConsumerRecord[K, V], Control])] {
  import TopicPartitionSourceActor._
  var kafka: ActorRef = _

  override def preStart(): Unit = {
    super.preStart()
    kafka = context.actorOf(Props(new KafkaActor(settings.createKafkaConsumer)))
    kafka ! KafkaActor.Subscribe(settings.topics)
  }

  override def postStop(): Unit = {
    children.values.foreach(context.stop)
    context.stop(kafka)
    super.postStop()
  }

  override def receive: Receive = normal

  def createSource(tp: TopicPartition): Source[ConsumerRecord[K, V], Control] = {
    Source.actorPublisher(Props(new TopicPartitionSubSourceActor(self, tp, kafka)))
      .mapMaterializedValue(ActorControl(_)(context.system))
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
    case KafkaActor.Assigned(tps) =>
      buffer = buffer.enqueue(tps.to[immutable.Iterable])
      pump()
    case KafkaActor.Revoked(tps) =>
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
    case ActorControl.Stop =>
      sender ! ActorControl.Stopped
      onComplete()
  }
}

object KafkaActor {
  //requests
  case class Subscribe(topics: Set[String])
  case class RequestMessages(topics: Set[TopicPartition])
  object RequestAnyMessages
  case class Commit(offsets: Map[TopicPartition, Long])
  //responses
  case class Assigned(partition: List[TopicPartition])
  case class Revoked(partition: List[TopicPartition])
  case class Messages[K, V](messages: Iterator[ConsumerRecord[K, V]])
  case class Committed(offsets: Map[TopicPartition, OffsetAndMetadata])
  //internal
  private case object Poll
}

class KafkaActor[K, V](consumerFactory: () => KafkaConsumer[K, V]) extends Actor {

  import KafkaActor._

  import scala.collection.JavaConversions._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  def pollTimeout() = 100.millis

  def pollDelay() = 100.millis

  private var requests = List.empty[(Set[TopicPartition], ActorRef)]
  var consumer: KafkaConsumer[K, V] = _

  def receive: Receive = LoggingReceive {
    case Commit(offsets) =>
      val commitMap = offsets.mapValues(new OffsetAndMetadata(_))
      val reply = sender
      consumer.commitAsync(commitMap, new OffsetCommitCallback {
        override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
          if (exception != null) reply ! Status.Failure(exception)
          else reply ! Committed(offsets.asScala.toMap)
        }
      })
      //right now we can not store commits in consumer - https://issues.apache.org/jira/browse/KAFKA-3412
      poll()

    case Subscribe(topics) =>
      val reply = sender
      consumer.subscribe(topics.toList, new ConsumerRebalanceListener {
        override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
          reply ! Assigned(partitions.toList)
        }

        override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
          //todo remove these topics from requests
          reply ! Revoked(partitions.toList)
        }
      })
    case Poll => poll()
    case RequestMessages(topics) => requests ::= (topics -> sender)
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

  def poll() = {
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

    if (requests.isEmpty) {
      schedulePoll()
    }
    else {
      self ! Poll
    }
  }

  var scheduled: Option[Cancellable] = None
  def schedulePoll(): Unit = {
    if (scheduled.isEmpty) {
      import context.dispatcher
      scheduled = Some(context.system.scheduler.scheduleOnce(pollDelay(), self, Poll))
    }
  }
}

object ActorControl {
  case object Stop
  case object Stopped
}
case class ActorControl(ref: ActorRef)(implicit as: ActorSystem) extends Control {
  import ActorControl._
  val _shutdown = Promise[Done]
  val _stop = Promise[Done]

  val proxyActor = as.actorOf(Props(new Actor {
    override def receive: Receive = {
      case Terminated(`ref`) => _shutdown.trySuccess(Done)
      case Stopped => _stop.trySuccess(Done)
    }
  }))
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
