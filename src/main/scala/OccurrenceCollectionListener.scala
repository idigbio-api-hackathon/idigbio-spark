import java.util

import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListener}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

class OccurrenceCollectionListener extends SparkListener {
  val props = new util.HashMap[String, Object]()
  val topic = "effechecka-selector"
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

  lazy val producer = new KafkaProducer[String, String](props)

  def sendMsg(msg: String): Unit = {
    val message = new ProducerRecord[String, String](topic, null, msg)
    producer.send(message)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    sendMsg("""onApplicationStart""")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    sendMsg("""onApplicationEnd""")
  }

}
