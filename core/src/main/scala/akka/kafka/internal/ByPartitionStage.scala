/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util

import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection._
import scala.concurrent.Future

class ByPartitionStage[K, V](settings: ConsumerSettings[K, V])
  extends GraphStageWithMaterializedValue[SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], Control])], NotUsed] {
  val out = Outlet[(TopicPartition, Source[ConsumerRecord[K, V], Control])]("partitions")
  override val shape: SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], Control])] = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val logic = new PartitionStageLogic(shape, settings)
    (logic, NotUsed)
  }
}

class PartitionStageLogic[K, V](
  shape: SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], Control])],
  settings: ConsumerSettings[K, V]
) extends TimerGraphStageLogic(shape)
{
  lazy val kafkaPump: KafkaPump[K, V] = {
    val result = new KafkaPump(materializer, settings.createKafkaConsumer())
    result.subscribe(
      settings.topics.toList,
      getAsyncCallback(assigned).invoke,
      getAsyncCallback(revoked).invoke
    )
    result
  }

  private var sources = Map.empty[TopicPartition, Control]

  private val listenerBuffer = mutable.Queue.empty[TopicPartition]

  def registerControl = getAsyncCallback { in: (TopicPartition, Control) =>
    sources += in
  }

  private def assigned(partitions: Iterable[TopicPartition]) = {
    println(s"Assigned ${partitions.toArray.toSeq}")
    partitions.foreach { partition =>
      listenerBuffer.enqueue(partition)
      pump()
    }
  }

  private def revoked(partitions: Iterable[TopicPartition]) = {
    println(s"Revoked ${partitions.toArray.toSeq}")
    partitions.foreach { partition =>
      if (listenerBuffer.contains(partition)) {
        listenerBuffer.filter(_ != partition)
      } else {
        sources.get(partition) match {
          case Some(control) => control.shutdown()
          case _ => println("Unable to found partition")
        }
      }
    }
  }

  private def pump(): Unit = {
    if (isAvailable(shape.out) && listenerBuffer.nonEmpty) {
      val tp = listenerBuffer.dequeue
      val source = createSource(tp)
      push(shape.out, (tp, source))
      pump()
    }
  }

  private def createSource(tp: TopicPartition) = {
    val subSource = Source.fromGraph(new SubSource[K, V](kafkaPump, tp, this))
    subSource
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit = {
      pump()
    }
  })

  override def preStart(): Unit = {
    super.preStart()
    kafkaPump.schedulePoll()
  }
}


object NoControl extends Control {
  override def stop(): Future[Done] = ???
  override def shutdown(): Future[Done] = ???
  override def isShutdown: Future[Done] = ???
}

class SubSource[K, V](kafkaPump: KafkaPump[K, V], tp: TopicPartition, parentLogic: PartitionStageLogic[K, V])
  extends GraphStageWithMaterializedValue[SourceShape[ConsumerRecord[K, V]], Control] {
  val out = Outlet[ConsumerRecord[K, V]]("messages")
  override val shape = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = new GraphStageLogic(shape) with Control {
      var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pump()
          if (isAvailable(out)) {
            kafkaPump.request(List(tp), handleIncomingCallback.invoke)
          }
        }
      })

      @tailrec
      def pump(): Unit = {
        if (isAvailable(out) && buffer.hasNext) {
          val next = buffer.next()
          push(out, next)
          pump()
        }
      }

      var handleIncomingCallback = getAsyncCallback(handleIncoming)

      def handleIncoming(iterator: Iterator[ConsumerRecord[K, V]]) = {
        println(s"Got incoming for $tp")
        require(buffer.isEmpty)
        buffer = iterator
        pump()
      }

      override def preStart(): Unit = {
        super.preStart()
        parentLogic.registerControl.invoke((tp, this))
      }

      val stopCallback = getAsyncCallback[Unit](_ => complete(out))
      override def stop(): Future[Done] = {
        println(s"Stopping $tp")
        stopCallback.invoke(())
        Future.successful(Done)
      }
      override def shutdown(): Future[Done] = stop()
      override def isShutdown: Future[Done] = Future.successful(Done)
    }
    (logic, logic)
  }
}

class KafkaPump[K, V](m: => Materializer, consumer: KafkaConsumer[K, V]) {

  import scala.collection.JavaConversions._
  import scala.concurrent.duration._

  def pollTimeout() = 100.millis

  def pollDelay() = 100.millis

  def subscribe(
                 topics: List[String],
                 assignCallback: Iterable[TopicPartition] => Unit,
                 revokeCallback: Iterable[TopicPartition] => Unit
               ) = synchronized {
    consumer.subscribe(topics, new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        assignCallback(partitions)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        revokeCallback(partitions)
      }
    })
  }

  type RequestCallback = Iterator[ConsumerRecord[K, V]] => Unit
  private var requests = List.empty[(List[TopicPartition], RequestCallback)]

  def request(topics: List[TopicPartition], callback: RequestCallback) = synchronized {
    requests = (topics -> callback) :: requests
  }

  def schedulePoll(): Unit = synchronized {
    m.scheduleOnce(pollDelay(), new Runnable {
      override def run(): Unit = poll()
    })
  }

  def poll(): Unit = synchronized {
//    println(s"Poll - ${requests.flatMap(_._1)}")

    val partitionsToFetch = requests.flatMap(_._1).toSet
    consumer.assignment().foreach { tp =>
      if (partitionsToFetch.contains(tp)) consumer.resume(tp)
      else consumer.pause(tp)
    }

    val result = consumer
      .poll(pollTimeout().toMillis)
      .groupBy(x => (x.topic(), x.partition()))

    val (nonEmptyTP, emptyTP) = requests.map {
      case (topics, callback) =>
        val messages = topics.toIterator.flatMap { tp =>
          result.getOrElse((tp.topic(), tp.partition()), Iterator.empty)
        }
        (topics, callback, messages)
    }.partition(_._3.hasNext)

    nonEmptyTP.foreach { case (_, callback, messages) => callback(messages) }
    requests = emptyTP.map { case (tp, callback, _) => (tp, callback) }

    schedulePoll()
  }
}
