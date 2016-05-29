/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import akka.NotUsed
import akka.kafka.Subscription.{TopicSubscription, TopicSubscriptionPattern}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.KafkaAsyncConsumer
import akka.kafka.{AutoSubscription, ConsumerSettings}
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream.{ActorMaterializer, Attributes, Outlet, SourceShape}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.{Failure, Success, Try}

private[kafka] abstract class SubSourceLogic[K, V, Msg](
    shape: SourceShape[(TopicPartition, Source[Msg, NotUsed])],
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription
) extends GraphStageLogic(shape) with PromiseControl with MessageBuilder[K, V, Msg] {
  var consumer: KafkaAsyncConsumer[K, V] = _
  var buffer: immutable.Queue[TopicPartition] = immutable.Queue.empty
  var subSources: Map[TopicPartition, Control] = immutable.Map.empty
  implicit lazy val ec = ActorMaterializer.downcast(materializer).executionContext

  override def preStart(): Unit = {
    super.preStart()
    consumer = KafkaAsyncConsumer[K, V](settings)(ActorMaterializer.downcast(materializer).system)
    subscription match {
      case TopicSubscription(topics) =>
        consumer.subscribe(topics, KafkaAsyncConsumer.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke))
      case TopicSubscriptionPattern(topics) =>
        consumer.subscribe(topics, KafkaAsyncConsumer.rebalanceListener(partitionAssignedCB.invoke, partitionRevokedCB.invoke))
    }
  }

  val partitionAssignedCB = getAsyncCallback[Iterable[TopicPartition]] { tps =>
    buffer = buffer.enqueue(tps.toList)
    pump()
  }
  val partitionRevokedCB = getAsyncCallback[Iterable[TopicPartition]] { r =>
    r.map(subSources.get).foreach(_.foreach(_.shutdown()))
    subSources --= r
  }

  val subsourceCancelledCB = getAsyncCallback[TopicPartition] { tp =>
    subSources -= tp
    buffer :+= tp
    pump()
  }

  val subsourceStartedCB = getAsyncCallback[(TopicPartition, Control)] {
    case (tp, control) => subSources += (tp -> control)
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit = pump()
  })

  def createSource(tp: TopicPartition): Source[Msg, NotUsed] = {
    Source.fromGraph(new SubSourceStage(tp, consumer))
  }

  @tailrec
  private def pump(): Unit = {
    if (buffer.nonEmpty && isAvailable(shape.out)) {
      val (tp, remains) = buffer.dequeue
      buffer = remains
      push(shape.out, (tp, createSource(tp)))
      pump()
    }
  }

  override def postStop(): Unit = {
    subSources.foreach {
      case (_, control) => control.shutdown()
    }
    consumer.close().onSuccess { case _ => shutdowned() }
    super.postStop()
  }

  val stopCB = getAsyncCallback[Unit]({ _ =>
    subSources.foreach {
      case (_, control) => control.stop()
    }
    completeStage()
  })

  val performStop = getAsyncCallback[Unit]({ _ =>
    complete(shape.out)
    stopped()
  }).invoke _

  val performShutdown = getAsyncCallback[Unit]({ _ =>
    completeStage()
  }).invoke _

  class SubSourceStage(tp: TopicPartition, consumer: KafkaAsyncConsumer[K, V]) extends GraphStage[SourceShape[Msg]] {
    val out = Outlet[Msg]("out")
    val shape = new SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) with PromiseControl {
        var requested = false
        var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
        implicit val ec = ActorMaterializer.downcast(materializer).executionContext

        override def preStart(): Unit = {
          subsourceStartedCB.invoke((tp, this))
        }

        override def postStop(): Unit = {
          shutdowned()
          super.postStop()
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

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pump()
          }

          override def onDownstreamFinish(): Unit = {
            subsourceCancelledCB.invoke(tp)
            super.onDownstreamFinish()
          }
        })

        val performStop = getAsyncCallback[Unit]({ _ =>
          complete(shape.out)
          stopped()
        }).invoke _

        val performShutdown = getAsyncCallback[Unit]({ _ =>
          completeStage()
        }).invoke _

        @tailrec
        private def pump(): Unit = {
          if (isAvailable(out)) {
            if (buffer.hasNext) {
              val msg = buffer.next()
              push(out, createMessage(msg))
              pump()
            }
            else if (!requested) {
              requested = true
              consumer.request(Set(tp)).onComplete(messagesCB.invoke)
            }
          }
        }
      }
    }
  }
}
