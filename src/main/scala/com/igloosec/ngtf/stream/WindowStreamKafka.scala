package com.igloosec.ngtf.stream

import java.text.SimpleDateFormat
import java.util.{Properties, Date}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 5분마다 5초 단위의 계산 결과 데이터를 hive 테이블에 저장하기 (windowStream 사용)
  * 상태 코드별 임계치 초과시 경보 발생
  *
  * hive table shcema
  * create table default.zz_statcode (code String, cnt BIGINT, event_time String)
  * ROW FORMAT DELIMITED
  * FIELDS TERMINATED BY ','
  * LOCATION '/user/hive/warehouse/zz_statcode';
  *
  * Created by ghyoo on 2016-02-16.
  */
object WindowStreamKafka {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val indexStatusCode = 8
  val batchDuration = 5
  val windowDuration = batchDuration * 60

  val thresholds = Map("200" -> 182000
    , "400" -> 26000
    , "500" -> 53000)


  def main(args: Array[String]): Unit = {

    val schema =
      StructType(
        StructField("code", StringType, true)
          :: StructField("cnt", LongType, true)
          :: StructField("event_time", StringType, true)
          :: Nil
      )


    val conf = new SparkConf().setAppName("WindowStreamingApp") //.setMaster("local[8]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration))

    val topics = List(("ApacheLog", 2)).toMap
    val streamData = KafkaUtils.createStream(ssc, "mn2.igloosecurity.co.kr,mn1.igloosecurity.co.kr,mn3.igloosecurity.co.kr", "cloudera_mirrormaker", topics)
    val logsWindow = streamData.window(Seconds(windowDuration), Seconds(windowDuration))


    //hiveContext.setConf("hive.metastore.uris", "thrift://mn3:9083")

    val outputTopic = "ApacheLogAlert"
    val brokers = "en1.igloosecurity.co.kr:9092,en2.igloosecurity.co.kr:9092,en3.igloosecurity.co.kr:9092"
    val props = new Properties()

    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)


    // 5sec data to kafka
    streamData.map(m => m._2.split(" "))
      .map(m => (m(indexStatusCode), 1L))
      .reduceByKey(_ + _)
      .foreachRDD((rdd, time) => {
        val arr = rdd.collect()


        arr.foreach((row : Tuple2[String, Long]) => {
          producer.send(new KeyedMessage[String, String](outputTopic, time.toString, sdf.format(new Date()) + "," + row._1.toString + "," + row._2.toString)  )
        })
      })

    // 5min/5min timewindow to hive
    val hiveContext = new HiveContext(sc)
    logsWindow.map(m => m._2.split(" "))
      .map(m => (m(indexStatusCode), 1L))
      .reduceByKeyAndWindow(_ + _, Seconds(windowDuration))
      .foreachRDD(rdd => {

        val rowRDD = rdd.map(rdd => Row(rdd._1, rdd._2.toLong, sdf.format(new Date())))
        val df = hiveContext.createDataFrame(rowRDD, schema).toDF()
        df.registerTempTable("summary")

        val result = hiveContext.sql("SELECT code, cnt, event_time FROM summary")
        result.write
          .format("parquet")
          .mode(SaveMode.Append)
          .saveAsTable("default.zz_statcode")

        result.foreach(println)
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
