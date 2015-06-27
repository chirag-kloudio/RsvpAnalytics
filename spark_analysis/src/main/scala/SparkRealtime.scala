import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.log4j.{Level, Logger}


object SparkRealtime {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf(true)
      .setMaster("local[2]")
      .setAppName("RSVP Live Stream")

    val ssc = new StreamingContext(conf, Seconds(10))

    val zk_quorum = "localhost:2181" // Zookeeper
    val group_id = "meetup_rsvp_stream"
    val topics = Map("rsvp_stream" -> 1)
    val json_stream = KafkaUtils.createStream(ssc, zk_quorum, group_id, topics)

    json_stream.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
