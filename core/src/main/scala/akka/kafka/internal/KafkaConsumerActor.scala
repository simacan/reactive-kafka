/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util
import java.util.regex.Pattern

import akka.Done
import akka.actor.{Actor, ActorRef, Cancellable, Props, Status}
import akka.event.LoggingReceive
import akka.kafka.ConsumerSettings
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.Promise

object KafkaConsumerActor {
  def props[K, V](settings: ConsumerSettings[K, V]) = Props(new KafkaConsumerActor(settings))

  //requests
  private[kafka] case class Assign(tps: Set[TopicPartition])
  private[kafka] case class AssignWithOffset(tps: Map[TopicPartition, Long])
  private[kafka] case class Subscribe(topics: Set[String], listener: ConsumerRebalanceListener)
  private[kafka] case class SubscribePattern(pattern: String, listener: ConsumerRebalanceListener)
  private[kafka] case class RequestMessages(topics: Set[TopicPartition])
  private[kafka] case object Watch
  private[kafka] case class Commit(offsets: Map[TopicPartition, Long])
  //responses
  private[kafka] case class Assigned(partition: List[TopicPartition])
  private[kafka] case class Revoked(partition: List[TopicPartition])
  private[kafka] case class Messages[K, V](messages: Iterator[ConsumerRecord[K, V]])
  private[kafka] case class Committed(offsets: Map[TopicPartition, OffsetAndMetadata])
  //internal
  private case object Poll
}

private[kafka] class KafkaConsumerActor[K, V](settings: ConsumerSettings[K, V]) extends Actor {
  import KafkaConsumerActor._

  def pollTimeout() = settings.pollTimeout
  def pollInterval() = settings.pollInterval

  var requests = Map.empty[TopicPartition, ActorRef]
  var consumer: KafkaConsumer[K, V] = _
  var finished: Promise[Done] = _
  var nextScheduledPoll: Option[Cancellable] = None
  var pollExpected = false

  def receive: Receive = LoggingReceive {
    case Watch => sender() ! finished.future
    case Assign(tps) =>
      val previousAssigned = consumer.assignment()
      consumer.assign((tps.toSeq ++ previousAssigned.asScala).asJava)
    case AssignWithOffset(tps) =>
      consumer.assign(tps.keys.toSeq.asJava)
      tps.foreach {
        case (tp, offset) => consumer.seek(tp, offset)
      }
    case Commit(offsets) =>
      val commitMap = offsets.mapValues(new OffsetAndMetadata(_))
      val reply = sender()
      consumer.commitAsync(commitMap.asJava, new OffsetCommitCallback {
        override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
          if (exception != null) reply ! Status.Failure(exception)
          else reply ! Committed(offsets.asScala.toMap)
        }
      })
      //right now we can not store commits in consumer - https://issues.apache.org/jira/browse/KAFKA-3412
      pollExpected = true
      poll()
    case Subscribe(topics, listener) =>
      consumer.subscribe(topics.toList.asJava, listener)
    case SubscribePattern(pattern, listener) =>
      consumer.subscribe(Pattern.compile(pattern), listener)
    case Poll =>
      pollExpected = true
      poll()
    case RequestMessages(topics) =>
      requests ++= topics.map(_ -> sender()).toMap
      pollExpected = true
      poll()
  }

  override def preStart(): Unit = {
    super.preStart()
    finished = Promise()
    consumer = settings.createKafkaConsumer()
    schedulePoll()
  }

  override def postStop(): Unit = {
    nextScheduledPoll.foreach(_.cancel())
    consumer.close()
    finished.trySuccess(Done)
    super.postStop()
  }

  def poll() = {
    if (pollExpected) {
      pollExpected = false
      nextScheduledPoll.foreach(_.cancel())
      nextScheduledPoll = None

      //set partitions to fetch
      val partitionsToFetch = requests.keys.toSet
      consumer.assignment().asScala.foreach { tp =>
        if (partitionsToFetch.contains(tp)) consumer.resume(tp)
        else consumer.pause(tp)
      }

      val rawResult = consumer.poll(pollTimeout().toMillis)
      if (!rawResult.isEmpty) {
        // split tps by reply actor
        val replyByTP = requests.toSeq
          .map { case (tp, ref) => (ref, tp) } //inverse map
          .groupBy(_._1) //group by reply
          .map { case (ref, refAndTps) => (ref, refAndTps.map { case (_ref, tps) => tps }) } //leave only tps

        //send messages to actors
        replyByTP.foreach {
          case (ref, tps) =>
            //gather all messages for ref
            val messages = tps.foldLeft[Iterator[ConsumerRecord[K, V]]](Iterator.empty) {
              case (acc, tp) =>
                val tpMessages = rawResult.records(tp).asScala.iterator
                if (acc.isEmpty) tpMessages
                else acc ++ tpMessages
            }
            if (messages.nonEmpty) {
              ref ! Messages(messages)
            }
        }
        //check the we got only requested partitions and did not drop any messages
        require((rawResult.partitions().asScala -- requests.keys).isEmpty)

        //remove tps for which we got messages
        requests --= rawResult.partitions().asScala
      }
      schedulePoll()
    }
  }

  def schedulePoll(): Unit = {
    if (nextScheduledPoll.isEmpty) {
      import context.dispatcher
      nextScheduledPoll = Some(context.system.scheduler.scheduleOnce(pollInterval(), self, Poll))
    }
  }
}
