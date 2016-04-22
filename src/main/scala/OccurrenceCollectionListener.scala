import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

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

  var started: AtomicBoolean = new AtomicBoolean(false)
  var startTime: Long = 0L
  var totalSubmittedTasks: AtomicLong = new AtomicLong(0L)
  var totalCompletedTasks: AtomicLong = new AtomicLong(0L)

  def sendMsg(msg: String): Unit = {
    val message = new ProducerRecord[String, String](topic, null, msg)
    producer.send(message)
  }

  override def onStageSubmitted(stage: SparkListenerStageSubmitted): Unit = {
    totalSubmittedTasks.getAndAdd(stage.stageInfo.numTasks)
  }

  override def onTaskEnd(task: SparkListenerTaskEnd): Unit = {
    def timeToString(remainingTimeApproxMin: Float): String = {
      "%.1f".format(remainingTimeApproxMin)
    }
    if (task.taskInfo.successful) {
      totalCompletedTasks.incrementAndGet()

      if (task.taskInfo.index % 100 == 0) {
        val totalCompletedSnapshot = totalCompletedTasks.get
        val totalSubmittedSnapshot = totalSubmittedTasks.get
        val percentComplete = totalCompletedSnapshot * 100 / totalSubmittedSnapshot
        sendMsg(s"${percentComplete}% (${totalCompletedSnapshot}/${totalSubmittedSnapshot}) complete")

        val totalDuration = task.taskInfo.finishTime - startTime
        val avgDurationPerTask = totalDuration / totalCompletedSnapshot.toFloat
        val remainingTimeApproxMs = (totalSubmittedSnapshot - totalCompletedSnapshot) * avgDurationPerTask
        val remainingTimeApproxMin: Float = remainingTimeApproxMs / (1000 * 60)
        sendMsg(s"eta +[${timeToString(remainingTimeApproxMin)}] minutes, started [${timeToString(processingTime.toFloat)}] minutes ago")
      }
    }

  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // onApplicationStart not sent to embedded listener, so using first job instead
    if (!started.getAndSet(true)) {
      startTime = jobStart.time
    }
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
