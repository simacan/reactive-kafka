package examples

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
import java.math.BigInteger

import akka.actor.{ActorSystem, Props}
import akka.kafka.ConsumerSettings
import akka.kafka.internal.ByPartitionActor
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

object ByPartitionExample extends App {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  implicit val m = ActorMaterializer(ActorMaterializerSettings(as).withInputBuffer(1, 1))

  import scala.collection.JavaConversions._

  val settings =  ConsumerSettings
    .create(as, new ByteArrayDeserializer, new StringDeserializer, Set("proto4.bss"))
    .withBootstrapServers("k1.c.test:9092")
    .withClientId(System.currentTimeMillis().toString)
    .withGroupId("test1")

  val x = Source
    .actorPublisher[(TopicPartition, Source[ConsumerRecord[Array[Byte], String], Control])](Props(new ByPartitionActor(settings)))
    .map {
      case (tp, s) =>
        println(s"Starting - $tp")
        s.map { x =>
            println(s"Got message - ${x.topic()}, ${x.partition()}, ${new BigInteger(x.key()).longValue()}")
            Thread.sleep(200)
            x
          }
          .toMat(Sink.ignore)(Keep.right)
          .mapMaterializedValue((tp, _))
          .run()
    }
    .map { case (tp, f) => f.onComplete(result => println(s"$tp finished with $result")); tp }
    .to(Sink.ignore)
    .run()

}
