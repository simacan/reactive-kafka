package examples

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
import java.math.BigInteger

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.kafka.internal.{SourceActor, TopicPartitionSourceActor}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

object UsualSourceExample extends App {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  implicit val m = ActorMaterializer(ActorMaterializerSettings(as).withInputBuffer(1, 1))

  import scala.collection.JavaConversions._

  val settings = ConsumerSettings
    .create(as, new ByteArrayDeserializer, new StringDeserializer, Set("proto4.bss"))
    .withBootstrapServers("k1.c.test:9092")
    .withClientId(System.currentTimeMillis().toString)
    .withGroupId("test1")

  val (control, f) = SourceActor[Array[Byte], String](settings)
    .map { x => println(x); x }
    .take(10)
    .toMat(Sink.ignore)(Keep.both)
    .run()

  f.onComplete(x => println(x))
  Thread.sleep(10000)
  control.shutdown()

}
