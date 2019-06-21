import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaProducer {
  def main(args:Array[String]): Unit ={
    //zookeeper连接属性
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka1:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    //通过zookeeper建立kafka的producer
    val producer = new KafkaProducer[String,String](props)
    //通过producer发送一些消息

    val message = new ProducerRecord[String,String]("test",null,"wo")
    //发送消息
    producer.send(message)

    producer.close()
    }


}
