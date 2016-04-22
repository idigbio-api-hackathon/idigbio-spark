import java.util

import org.apache.spark.executor.{OutputMetrics, TaskMetrics}
import org.apache.spark.scheduler._
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

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    sendMsg(s"onStageCompleted ${stageCompleted.stageInfo.name}")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    sendMsg(s"onStageSubmitted ${stageSubmitted.stageInfo.name}")
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    sendMsg(s"onTaskStart with stageId [${taskStart.stageId}] status: [${taskStart.taskInfo.status}]")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    sendMsg(s"onTaskEnd with stageId [${taskEnd.stageId}] status: [${taskEnd.taskInfo.status}]")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    sendMsg(s"onJobStart job id [${jobStart.jobId}] with stages [${jobStart.stageIds}] at [${jobStart.time}]")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    jobEnd.jobResult match {
      case JobSucceeded => {
        sendMsg(s"onJobEnd job id [${jobEnd.jobId}] succeeded at [${jobEnd.time}]")
      }
      case _ => {
        sendMsg(s"onJobEnd job id [${jobEnd.jobId}] failed")
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    sendMsg(s"onApplicationEnd end time: [${applicationEnd.time}]")
  }
}
