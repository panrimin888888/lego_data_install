package com.atguigu.spark.streaming.project.util
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils




object MyKafkaUtil {
  //kafka消费者配置
  val kafkaParam = Map(
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "group.id" -> "atguigu"
  )
  def getKafkaStream(ssc: StreamingContext,topic: String,otherTopic: String*)={
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaParam,
      (otherTopic:+topic).toSet
    ).map(_._2)
  }

}
