/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import akka.{Done, NotUsed}
import akka.kafka.{AutoSubscription, ConsumerSettings, Subscription}
import akka.kafka.scaladsl.{Consumer, KafkaAsyncConsumer}
import akka.kafka.scaladsl.Consumer._
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * INTERNAL API
 */
private[kafka] object ConsumerStage {
  def plainSubSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription) = {
    new KafkaSourceStage[K, V, (TopicPartition, Source[ConsumerRecord[K, V], NotUsed])] {
      override protected def logic(shape: SourceShape[(TopicPartition, Source[ConsumerRecord[K, V], NotUsed])]) =
        new SubSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription) with PlainMessageBuilder[K, V]
    }
  }

  def committableSubSource[K, V](settings: ConsumerSettings[K, V], subscription: AutoSubscription) = {
    new KafkaSourceStage[K, V, (TopicPartition, Source[CommittableMessage[K, V], NotUsed])] {
      override protected def logic(shape: SourceShape[(TopicPartition, Source[CommittableMessage[K, V], NotUsed])]) =
        new SubSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription) with CommittableMessageBuilder[K, V] {
          override def clientId: String = settings.properties(ConsumerConfig.CLIENT_ID_CONFIG)
          lazy val committer: Committer = new KafkaAsyncConsumerCommitter(consumer)(ec)
        }
    }
  }

  def plainSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription) = {
    new KafkaSourceStage[K, V, ConsumerRecord[K, V]] {
      override protected def logic(shape: SourceShape[ConsumerRecord[K, V]]) =
        new SingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, settings, subscription) with PlainMessageBuilder[K, V]
    }
  }

  def externalPlainSource[K, V](consumer: KafkaAsyncConsumer[K, V], subscription: Subscription) = {
    new KafkaSourceStage[K, V, ConsumerRecord[K, V]] {
      override protected def logic(shape: SourceShape[ConsumerRecord[K, V]]) =
        new ExternalSingleSourceLogic[K, V, ConsumerRecord[K, V]](shape, consumer, subscription) with PlainMessageBuilder[K, V]
    }
  }

  def committableSource[K, V](settings: ConsumerSettings[K, V], subscription: Subscription) = {
    new KafkaSourceStage[K, V, CommittableMessage[K, V]] {
      override protected def logic(shape: SourceShape[CommittableMessage[K, V]]) =
        new SingleSourceLogic[K, V, CommittableMessage[K, V]](shape, settings, subscription) with CommittableMessageBuilder[K, V] {
          override def clientId: String = settings.properties(ConsumerConfig.CLIENT_ID_CONFIG)
          lazy val committer: Committer = new KafkaAsyncConsumerCommitter(consumer)(ec)
        }
    }
  }

  def externalCommittableSource[K, V](consumer: KafkaAsyncConsumer[K, V], _clientId: String, subscription: Subscription) = {
    new KafkaSourceStage[K, V, CommittableMessage[K, V]] {
      override protected def logic(shape: SourceShape[CommittableMessage[K, V]]) =
        new ExternalSingleSourceLogic[K, V, CommittableMessage[K, V]](shape, consumer, subscription) with CommittableMessageBuilder[K, V] {
          override def clientId: String = _clientId
          lazy val committer: Committer = new KafkaAsyncConsumerCommitter(consumer)(ec)
        }
    }
  }

  class KafkaAsyncConsumerCommitter(client: KafkaAsyncConsumer[_, _])(implicit ec: ExecutionContext) extends Committer {
    override def commit(offset: PartitionOffset): Future[Done] = {
      client.commit(Map(
        new TopicPartition(offset.key.topic, offset.key.partition) -> (offset.offset + 1)
      )).map(_ => Done)
    }
    override def commit(batch: CommittableOffsetBatchImpl): Future[Done] = {
      client.commit(batch.offsets.map {
        case (ctp, offset) => new TopicPartition(ctp.topic, ctp.partition) -> (offset + 1)
      }).map(_ => Done)
    }
  }

  abstract class KafkaSourceStage[K, V, Msg]()
      extends GraphStageWithMaterializedValue[SourceShape[Msg], Control] {
    protected val out = Outlet[Msg]("out")
    val shape = new SourceShape(out)
    protected def logic(shape: SourceShape[Msg]): GraphStageLogic with Control
    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
      val result = logic(shape)
      (result, result)
    }
  }

  private trait PlainMessageBuilder[K, V] extends MessageBuilder[K, V, ConsumerRecord[K, V]] {
    override def createMessage(rec: ConsumerRecord[K, V]) = rec
  }

  private trait CommittableMessageBuilder[K, V] extends MessageBuilder[K, V, CommittableMessage[K, V]] {
    def clientId: String
    def committer: Committer
    def ec: ExecutionContext

    override def createMessage(rec: ConsumerRecord[K, V]) = {
      val offset = Consumer.PartitionOffset(
        ClientTopicPartition(
          clientId = clientId,
          topic = rec.topic,
          partition = rec.partition
        ),
        offset = rec.offset
      )
      Consumer.CommittableMessage(rec.key, rec.value, CommittableOffsetImpl(offset)(committer))
    }
  }

  final case class CommittableOffsetImpl(override val partitionOffset: Consumer.PartitionOffset)(val committer: Committer)
      extends Consumer.CommittableOffset {
    override def commit(): Future[Done] =
      committer.commit(partitionOffset)
  }

  trait Committer {
    def commit(offset: Consumer.PartitionOffset): Future[Done]
    def commit(batch: CommittableOffsetBatchImpl): Future[Done]
  }

  final class CommittableOffsetBatchImpl(val offsets: Map[ClientTopicPartition, Long], val stages: Map[String, Committer])
      extends CommittableOffsetBatch {

    override def updated(committableOffset: Consumer.CommittableOffset): CommittableOffsetBatch = {
      val partitionOffset = committableOffset.partitionOffset
      val key = partitionOffset.key

      val newOffsets = offsets.updated(key, committableOffset.partitionOffset.offset)

      val stage = committableOffset match {
        case c: CommittableOffsetImpl => c.committer
        case _ => throw new IllegalArgumentException(
          s"Unknow CommittableOffset, got [${committableOffset.getClass.getName}], " +
            s"expected [${classOf[CommittableOffsetImpl].getName}]"
        )
      }

      val newStages = stages.get(key.clientId) match {
        case Some(s) =>
          require(s == stage, s"CommittableOffset [$committableOffset] origin stage must be same as other " +
            s"stage with same clientId. Expected [$s], got [$stage]")
          stages
        case None =>
          stages.updated(key.clientId, stage)
      }

      new CommittableOffsetBatchImpl(newOffsets, newStages)
    }

    override def getOffset(key: ClientTopicPartition): Option[Long] =
      offsets.get(key)

    override def toString(): String =
      s"CommittableOffsetBatch(${offsets.mkString("->")})"

    override def commit(): Future[Done] = {
      if (offsets.isEmpty)
        Future.successful(Done)
      else {
        stages.head._2.commit(this)
      }
    }
  }
}

private[kafka] trait MessageBuilder[K, V, Msg] {
  def createMessage(rec: ConsumerRecord[K, V]): Msg
}

private[kafka] trait PromiseControl extends Control {
  val shutdownPromise: Promise[Done] = Promise()
  val stopPromise: Promise[Done] = Promise()

  def stopped() = stopPromise.trySuccess(Done)
  def shutdowned() = {
    stopPromise.trySuccess(Done)
    shutdownPromise.trySuccess(Done)
  }

  val performStop: Unit => Unit
  val performShutdown: Unit => Unit

  override def stop(): Future[Done] = {
    performStop(())
    stopPromise.future
  }
  override def shutdown(): Future[Done] = {
    performShutdown(())
    shutdownPromise.future
  }
  override def isShutdown: Future[Done] = shutdownPromise.future
}
