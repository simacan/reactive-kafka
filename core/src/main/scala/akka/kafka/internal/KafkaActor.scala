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

  def pollTimeout() = 10.millis

  def pollDelay() = 100.millis

  private var requests = List.empty[(Set[TopicPartition], ActorRef)]
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

  def poll(forceSchedule: Boolean = true) = {
    scheduled.foreach(_.cancel())
    scheduled = None

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
