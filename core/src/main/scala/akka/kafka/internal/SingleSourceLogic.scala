/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import akka.kafka.Subscription.{Assignment, AssignmentWithOffset, TopicSubscription, TopicSubscriptionPattern}
import akka.kafka.scaladsl.KafkaAsyncConsumer
import akka.kafka.{ConsumerSettings, Subscription}
import akka.stream.stage.{GraphStageLogic, OutHandler}
import akka.stream.{ActorMaterializer, SourceShape}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

private[kafka] abstract class SingleSourceLogic[K, V, Msg](
    shape: SourceShape[Msg],
    settings: ConsumerSettings[K, V],
    subscription: Subscription
) extends GraphStageLogic(shape) with PromiseControl with MessageBuilder[K, V, Msg] {
  var consumer: KafkaAsyncConsumer[K, V] = _
  var tps = Set.empty[TopicPartition]
  var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
  var requested = false
  implicit lazy val ec = ActorMaterializer.downcast(materializer).executionContext

  override def preStart(): Unit = {
    super.preStart()
    setKeepGoing(true)
    consumer = KafkaAsyncConsumer[K, V](settings)(ActorMaterializer.downcast(materializer).system)
    subscription match {
      case TopicSubscription(topics) =>
        consumer.subscribe(topics, KafkaAsyncConsumer.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke))
      case TopicSubscriptionPattern(topics) =>
        consumer.subscribe(topics, KafkaAsyncConsumer.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke))
      case Assignment(topics) =>
        consumer.assign(topics)
        tps ++= topics
      case AssignmentWithOffset(topics) =>
        consumer.assign(topics)
        tps ++= topics.keySet
    }
  }

  val partitionAssignedCB = getAsyncCallback[Iterable[TopicPartition]] { newTps =>
    tps ++= newTps
    pump()
  }
  val partitionRevokedCB = getAsyncCallback[Iterable[TopicPartition]] { newTps =>
    tps --= newTps
    pump()
  }

  @tailrec
  private def pump(): Unit = {
    if (isAvailable(shape.out)) {
      if (buffer.hasNext) {
        val msg = buffer.next()
        push(shape.out, createMessage(msg))
        pump()
      }
      else if (!requested && tps.nonEmpty) {
        requested = true
        consumer.request(tps).onComplete(messagesCB.invoke)
      }
    }
  }

  val messagesCB = getAsyncCallback[Try[Iterator[ConsumerRecord[K, V]]]] {
    case Success(msgs) =>
      requested = false
      // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
      if (buffer.hasNext) {
        buffer = buffer ++ msgs
      }
      else {
        buffer = msgs
      }
      pump()
    case Failure(ex) => failStage(ex)
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit = {
      pump()
    }
  })

  val performStop = getAsyncCallback[Unit]({ _ =>
    complete(shape.out)
    stopped()
  }).invoke _

  val performShutdown = getAsyncCallback[Unit]({ _ =>
    completeStage()
  }).invoke _

  override def postStop(): Unit = {
    consumer.close().onSuccess { case _ => shutdowned() }
    super.postStop()
  }
}
