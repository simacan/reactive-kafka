/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.event.LoggingReceive
import akka.kafka.ConsumerSettings
import akka.kafka.internal.ConsumerStage.CommittableOffsetImpl
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.{ClientTopicPartition, CommittableMessage, Control}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.{Future, Promise}

trait MessageBuilder[K, V, MSG] {
  def createMessage(rec: ConsumerRecord[K, V]): MSG
}

trait PlainMessageBuilder[K, V] extends MessageBuilder[K, V, ConsumerRecord[K, V]] {
  self: SourceActor[K, V, ConsumerRecord[K, V]] =>

  override def createMessage(rec: ConsumerRecord[K, V]) = rec
}

trait CommittableMessageBuilder[K, V] extends MessageBuilder[K, V, CommittableMessage[K, V]] {
  self: SourceActor[K, V, CommittableMessage[K, V]] =>

  lazy val committer = ActorCommitter(consumer)(context.dispatcher)
  override def createMessage(rec: ConsumerRecord[K, V]) = {
    val offset = Consumer.PartitionOffset(
      ClientTopicPartition(
        clientId = settings.properties(ConsumerConfig.CLIENT_ID_CONFIG),
        topic = rec.topic,
        partition = rec.partition
      ),
      offset = rec.offset
    )
    Consumer.CommittableMessage(rec.key, rec.value, CommittableOffsetImpl(offset)(committer))
  }
}

abstract case class SourceActor[K, V, MSG](settings: ConsumerSettings[K, V])
    extends ActorPublisher[MSG]
    with MessageBuilder[K, V, MSG] {
  var consumer: ActorRef = _
  var tps = Set.empty[TopicPartition]
  var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

  override def preStart(): Unit = {
    super.preStart()
    consumer = context.actorOf(Props(new KafkaConsumerActor(settings.createKafkaConsumer)))
    consumer ! KafkaConsumerActor.Subscribe(settings.topics)
  }

  @tailrec
  private def pump(): Unit = {
    if (totalDemand > 0) {
      if (!buffer.hasNext) {
        consumer ! KafkaConsumerActor.RequestMessages(tps)
      }
      else {
        val msg = buffer.next()
        onNext(createMessage(msg))
        pump()
      }
    }
  }

  override def receive: Receive = working

  def working: Receive = LoggingReceive {
    case KafkaConsumerActor.Assigned(newTps) =>
      tps ++= newTps
      pump()
    case KafkaConsumerActor.Revoked(newTps) =>
      tps --= newTps
      pump()
    case KafkaConsumerActor.Messages(msgs) =>
      // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
      if (buffer.hasNext) {
        buffer = buffer ++ msgs.asInstanceOf[Iterator[ConsumerRecord[K, V]]]
      }
      else {
        buffer = msgs.asInstanceOf[Iterator[ConsumerRecord[K, V]]]
      }
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
    case KafkaConsumerActor.Assigned(newTps) =>
    case KafkaConsumerActor.Revoked(newTps) =>
    case KafkaConsumerActor.Messages(msgs) =>
    case ActorControl.Stop => sender() ! ActorControl.Stopped
    case Cancel => context.stop(self)
  }
}

object TopicPartitionSourceActor {
  case class RegisterSubSource(tp: TopicPartition)

  def apply[K, V](settings: ConsumerSettings[K, V])(implicit as: ActorSystem): Source[(TopicPartition, Source[ConsumerRecord[K, V], Control]), Control] = {
    Source.actorPublisher[(TopicPartition, Source[ConsumerRecord[K, V], Control])](Props(new TopicPartitionSourceActor(settings)))
      .mapMaterializedValue(ActorControl(_))
  }

  class SubSourceActor[K, V](mainSource: ActorRef, tp: TopicPartition, kafka: ActorRef)
      extends ActorPublisher[ConsumerRecord[K, V]] {
    var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
    override def receive: Receive = normal

    override def preStart(): Unit = {
      super.preStart()
      mainSource ! RegisterSubSource(tp)
    }

    @tailrec
    private def pump(): Unit = {
      if (buffer.hasNext && totalDemand > 0) {
        onNext(buffer.next())
        pump()
      }
    }

    def normal: Receive = LoggingReceive {
      case KafkaConsumerActor.Messages(msgs) =>
        val newMessages = msgs.asInstanceOf[Iterator[ConsumerRecord[K, V]]]
        buffer = buffer ++ newMessages
        pump()
      case Request(_) =>
        pump()
        if (!buffer.hasNext && totalDemand > 0) {
          kafka ! KafkaConsumerActor.RequestMessages(Set(tp))
        }
      case Cancel => context.stop(self)
      case ActorControl.Stop => onComplete()
    }
  }
}

class TopicPartitionSourceActor[K, V](settings: ConsumerSettings[K, V])
    extends ActorPublisher[(TopicPartition, Source[ConsumerRecord[K, V], Control])] {
  import TopicPartitionSourceActor._
  var kafka: ActorRef = _
  var buffer: immutable.Queue[TopicPartition] = immutable.Queue.empty
  var subSources: Map[TopicPartition, ActorRef] = immutable.Map.empty

  override def preStart(): Unit = {
    super.preStart()
    kafka = context.actorOf(Props(new KafkaConsumerActor(settings.createKafkaConsumer)))
    kafka ! KafkaConsumerActor.Subscribe(settings.topics)
  }

  override def postStop(): Unit = {
    subSources.values.foreach(context.stop)
    context.stop(kafka)
    super.postStop()
  }

  override def receive: Receive = LoggingReceive {
    case RegisterSubSource(tp) =>
      context.watch(sender)
      subSources += (tp -> sender)
    case KafkaConsumerActor.Assigned(tps) =>
      buffer = buffer.enqueue(tps)
      pump()
    case KafkaConsumerActor.Revoked(tps) =>
      tps.foreach { tp =>
        subSources.get(tp) match {
          case Some(ref) =>
            context.unwatch(ref)
            context.stop(ref)
          case None =>
            buffer = buffer.filter(x => tps.contains(x))
        }
      }
    case Terminated(ref) =>
      subSources.find(_._2 == ref) match {
        case Some((tp, _)) =>
          subSources -= tp
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

  def createSource(tp: TopicPartition): Source[ConsumerRecord[K, V], Control] = {
    Source.actorPublisher(Props(new SubSourceActor(self, tp, kafka)))
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
    override def preStart(): Unit = {
      context.watch(ref)
    }

    override def receive: Receive = {
      case Terminated(`ref`) =>
        _shutdown.trySuccess(Done)
        _stop.trySuccess(Done)
      case Stopped =>
        _stop.trySuccess(Done)
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
