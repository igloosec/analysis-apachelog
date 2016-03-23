package com.igloosec.ngtf.stream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ghyoo on 2016-03-14.
  */
object NetScreenEvent {

  val batchDuration = 500
  val windowDuration = batchDuration * 5

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetScreenEvent").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(batchDuration))

    val topics = List(("NetScreenLog", 1)).toMap
    val streamData = KafkaUtils.createStream(ssc, "mn2.igloosecurity.co.kr,mn1.igloosecurity.co.kr,mn3.igloosecurity.co.kr", "jh", topics, StorageLevel.MEMORY_AND_DISK_SER_2)

    val logsWindow = streamData.window(Milliseconds(windowDuration), Milliseconds(windowDuration))

    val pattern = ".+action=(\\S+).+src=(\\S+)".r
    logsWindow.map(log => pattern.findAllMatchIn(log._2).next())
      .filter(entry => entry.group(1).equals("Deny")) // action=Deny인 로그 필터링
      .map(entry => entry.group(2))
      .countByValue()
      .filter(cnt => cnt._2 > 20) // 발생 건수가 5,000건 이상인 이벤트 필터링
      .foreachRDD(rdd => rdd.foreach(item => println(item._1))) // 각 이벤트의 Source IP 출력

    ssc.start()
    ssc.awaitTermination()
  }
}
