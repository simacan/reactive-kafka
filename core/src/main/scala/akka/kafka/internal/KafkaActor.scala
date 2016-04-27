package akka.kafka.internal

import java.util

import akka.Done
import akka.actor.{Actor, ActorRef, Cancellable, Status}
import akka.event.LoggingReceive
import akka.kafka.internal.ConsumerStage.{CommittableOffsetBatchImpl, Committer}
import akka.kafka.scaladsl.Consumer.PartitionOffset
import akka.util.Timeout
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

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

  case class Committer(ref: ActorRef)(implicit ec: ExecutionContext) extends akka.kafka.internal.ConsumerStage.Committer {
    import akka.pattern.ask

    implicit val timeout = Timeout(1.minute)
    override def commit(offset: PartitionOffset): Future[Done] = {
      val result = ref ? KafkaActor.Commit(Map(
        new TopicPartition(offset.key.topic, offset.key.partition) -> (offset.offset + 1)
      ))
      result.map(_ => Done)
    }
    override def commit(batch: CommittableOffsetBatchImpl): Future[Done] = {
      val result = ref ? KafkaActor.Commit(batch.offsets.map {
        case (ctp, offset) => new TopicPartition(ctp.topic, ctp.partition) -> (offset + 1)
      })
      result.map(_ => Done)
    }
  }
}

class KafkaActor[K, V](consumerFactory: () => KafkaConsumer[K, V]) extends Actor {

  import KafkaActor._

  import scala.collection.JavaConversions._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  def pollTimeout() = 200.millis

  def pollDelay() = 500.millis

  private var requests = Map.empty[TopicPartition, ActorRef]
  var consumer: KafkaConsumer[K, V] = _

  def receive: Receive = LoggingReceive {
    case Commit(offsets) =>
      val commitMap = offsets.mapValues(new OffsetAndMetadata(_))
      val reply = sender
      consumer.commitAsync(commitMap, new OffsetCommitCallback {
        override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
          println(s"Commit completed $commitMap")
          if (exception != null) reply ! Status.Failure(exception)
          else reply ! Committed(offsets.asScala.toMap)
        }
      })
      //right now we can not store commits in consumer - https://issues.apache.org/jira/browse/KAFKA-3412
      poll(forceSchedule = false)

    case Subscribe(topics) =>
      val reply = sender
      consumer.subscribe(topics.toList, new ConsumerRebalanceListener {
        override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
          reply ! Assigned(partitions.toList)
        }

        override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
          reply ! Revoked(partitions.toList)
        }
      })
    case Poll => poll()
    case RequestMessages(topics) => requests ++= topics.map(_ -> sender()).toMap
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

  def poll(forceSchedule: Boolean = true) = {
    scheduled.foreach(_.cancel())
    scheduled = None

    val partitionsToFetch = requests.keys.toSet
    consumer.assignment().foreach { tp =>
      if (partitionsToFetch.contains(tp)) consumer.resume(tp)
      else consumer.pause(tp)
    }
    val rawResult = consumer.poll(pollTimeout().toMillis)

    //push this into separate actor or thread to provide maximum fetching speed
    val result = rawResult.groupBy(x => (x.topic(), x.partition()))

    // split tps by reply actor
    val replyByTP = requests.toSeq
      .map(x => (x._2, x._1)) //inverse map
      .groupBy(_._1) //group by reply
      .map { case (ref, refAndTps) => (ref, refAndTps.map { case (_ref, tps) => tps }) } //leave only tps

    //send messages to actors
    replyByTP.foreach { case (ref, tps) =>
      val messages = tps.foldLeft[Iterator[ConsumerRecord[K, V]]](Iterator.empty) {
        case (acc, tp) =>
          val tpMessages = result
            .get((tp.topic, tp.partition))
            .map(_.toIterator)
            .getOrElse(Iterator.empty)
          if(acc.isEmpty) tpMessages
          else acc ++ tpMessages
      }
      if(messages.nonEmpty) {
        ref ! Messages(messages)
      }
    }
    //remove tps for which we got messages
    requests --= result.keys.map(x => new TopicPartition(x._1, x._2))

    if (requests.isEmpty) {
      schedulePoll()
    }
    else if(forceSchedule) {
      self ! Poll
    }
  }

  var scheduled: Option[Cancellable] = None
  def schedulePoll(): Unit = {
    if (scheduled.isEmpty) {
      import context.dispatcher
      println("schedule poll")
      scheduled = Some(context.system.scheduler.scheduleOnce(pollDelay(), self, Poll))
    }
  }
}
