package akka.kafka.internal

import java.util

import akka.actor.Cancellable
import akka.{Done, NotUsed}
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler, TimerGraphStageLogic}
import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scala.collection._
import scala.concurrent.Future

class ByPartitionStage[K, V](
    settings: ConsumerSettings[K, V]
) extends GraphStageWithMaterializedValue[SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], Control])], NotUsed] {
  val out = Outlet[(TopicPartition, Source[ConsumerRecord[K, V], Control])]("partitions")
  override val shape = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val logic = new TimerGraphStageLogic(shape) with StageLogging {
      lazy val kafkaPump: KafkaPump[K, V] = {
        val result = new KafkaPump(materializer, settings.createKafkaConsumer())
        result.subscribe(settings.topics.toList, new ConsumerRebalanceListener {
          val assignCallback = getAsyncCallback(assigned)
          val revokeCallback = getAsyncCallback(revoked)
          override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = assignCallback.invoke(partitions)
          override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = revokeCallback.invoke(partitions)
        })
        result
      }

      var sources = Map.empty[TopicPartition, Control]

      var listenerBuffer = mutable.Queue.empty[TopicPartition]
      def assigned(partitions: Iterable[TopicPartition]) = {
        println(s"Assigned ${partitions.toArray.toSeq}")
        partitions.foreach { partition =>
          listenerBuffer.enqueue(partition)
          pump()
        }
      }
      def revoked(partitions: Iterable[TopicPartition]) = {
        println(s"Revoked ${partitions.toArray.toSeq}")
        partitions.foreach { partition =>
          sources.get(partition) match {
            case Some(source) =>
            case _ =>
          }
        }
      }
      def pump(): Unit = {
        if(isAvailable(out) && listenerBuffer.nonEmpty) {
          val tp = listenerBuffer.dequeue
          push(out, (tp, createSource(tp)))
          pump()
        }
      }


      def createSource(tp: TopicPartition) = {
        val subSource = Source.fromGraph(new SubSource[K, V]())
        subSource
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pump()
        }
      })

      override def preStart(): Unit = {
        super.preStart()
        kafkaPump.schedulePoll()
      }
    }
    (logic, NotUsed)
  }
}

object NoControl extends Control {
  override def stop(): Future[Done] = ???
  override def shutdown(): Future[Done] = ???
  override def isShutdown: Future[Done] = ???
}

class SubSource[K, V] extends GraphStageWithMaterializedValue[SourceShape[ConsumerRecord[K, V]], Control] {
  val out = Outlet[ConsumerRecord[K, V]]("partitions")
  override val shape = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val logic = new GraphStageLogic(shape) {
    }
    (logic, NoControl)
  }
}

class KafkaPump[K, V](m: => Materializer, consumer: KafkaConsumer[K, V]) {
  import scala.collection.JavaConversions._
  import scala.concurrent.duration._

  def pollTimeout() = 100.millis
  def pollDelay() = 100.millis

  def subscribe(topics: List[String], callback: ConsumerRebalanceListener) = {
    consumer.subscribe(topics, callback)
  }

  def schedulePoll(): Unit = {
    m.scheduleOnce(pollDelay(), new Runnable {
      override def run(): Unit = poll()
    })
  }

  def poll() = {
    println("doing poll")
    consumer.poll(pollTimeout().toMillis)
    schedulePoll()
  }
}


