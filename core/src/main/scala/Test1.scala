import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.kafka.internal.ByPartitionStage
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

/**
  * Created by alexey on 21.04.16.
  */
object Test1 extends App {
  implicit val as = ActorSystem()
  implicit val m = ActorMaterializer(ActorMaterializerSettings(as))

  import scala.collection.JavaConversions._

  Source.fromGraph(new ByPartitionStage[Array[Byte], String](
    ConsumerSettings
      .create(as, new ByteArrayDeserializer, new StringDeserializer, Set("proto4.bss"))
      .withBootstrapServers("k1.c.test:9092")
      .withGroupId("test1")
  ))
    .map { x =>
      println(x)
      x
    }
    .to(Sink.ignore)
    .run()

}
