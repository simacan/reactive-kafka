/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import akka.Done
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.{ClientTopicPartition, CommittableOffsetBatch}

import scala.concurrent.Future

/**
 * INTERNAL API
 */
private[kafka] object ConsumerStage {

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

