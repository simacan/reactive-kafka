package akka.kafka.internal

import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Source
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

/**
  * Created by al.romanchuk on 21.04.16.
  */
class ByPartitionStage[K, V](settings: ConsumerSettings[K, V], consumerProvider: () => KafkaConsumer[K, V])
  extends GraphStageWithMaterializedValue[SourceShape[(TopicPartition, Source[ConsumerRecord[K, V])], Consumer.Control] {
  val out = Outlet[ConsumerRecord[K, V]]("messages")
  override val shape = SourceShape(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = ???
}
