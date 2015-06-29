// This is under progress work. Will resume once I get enough time.

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap
import com.lambdaworks.jacks.JacksMapper
import org.apache.log4j.{Level, Logger}

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native._

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
    // create a DStream from Kafka
    val json_stream = KafkaUtils.createStream(ssc, zk_quorum, group_id, topics)//.map(_._2)

    json_stream.foreachRDD(rsvps => rsvps.foreach(rsvp => {

        val each_rsvp = JsonMethods.parse(rsvp.asJson)
      }
    ))

//    // parse json
//    val rsvps = json_stream.map(parse(_))

//    rsvps.foreachRDD(rdd => {
//      val venue = rdd.map(x => compact(render(x \ "venue")))
//      println(venue)
//    })


//    json_stream.foreachRDD(rdd => {
//      val rsvp = rdd.map(rsvp => rsvp._2) //Get json from rdd
//      //rsvp.foreach(println)
//      for (each_rsvp <- rsvp) {
//        val rsvp_map = JacksMapper.readValue[Map[String, AnyRef]](each_rsvp)
//        val venue = rsvp_map.get("venue")
//        if (venue != None) {
//          val venueMap = venue.get.toString
//          println(venueMap)
//
//
//
//        } else {
//          println("No Venue: " + venue)
//        }
//        val event = rsvp_map.get("event")
//        val guests = rsvp_map.get("guests")
//        val rsvp_id = rsvp_map.get("rsvp_id")
//        val response = rsvp_map.get("response")
//        val mtime = rsvp_map.get("mtime")
//        val member = rsvp_map.get("member")
//        val visibility = rsvp_map.get("visibility")
//        val group = rsvp_map.get("group")
//
//      }
//    })

    ssc.start()
    ssc.awaitTermination()

  }
}
