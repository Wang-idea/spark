package cn.wi.spark.kafka.pull

import java.sql.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingKafkaDirect
 * @Author: xianlawei
 * @Description: 继承kafka，采用Direct方式读取数据，对每批次（时间为1秒）数据进行词频统计，将统计结果输出到控制台
 * @date: 2019/8/31 13:10
 */
object StreamingKafkaDirect {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        //TODO: 设置每秒钟读取Kafka中Topic最大数据量
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")
      val context = new StreamingContext(sparkConf, Seconds(5))
      context.sparkContext.setLogLevel("WARN")
      context
    }

    //设置检查点目录，通过目录是HDFS上目录，将数据存储在HDFS上
    ssc.checkpoint("/spark/checkpoint/ckpt-0001")

    /**
     * def createDirectStream[
     * K: ClassTag, // Topic中Key数据类型
     * V: ClassTag,
     * KD <: Decoder[K]: ClassTag, // 表示Topic中Key数据解码（从文件中读取，反序列化）
     * VD <: Decoder[V]: ClassTag] (
     * ssc: StreamingContext,
     * kafkaParams: Map[String, String],
     * topics: Set[String]
     * ): InputDStream[(K, V)]
     */
    //表示从Kafka Topic中读取数据时相关参数设置
    val kafkaParams: Map[String, String] = Map("bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      //表示从Topic的各个分区的哪个偏移量开始消费时局，设置为最大的偏移量开始消费数据
      "auto.offset.reset" -> "largest")

    //从哪些Topic中消费时局  set集合可以去重
    val topics: Set[String] = Set("testTopic")

    //采用Direct方式从Kafka的Topic中读取数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        topics
      )

    //获取从Kafka读取数据的Message
    val inputDStream: DStream[String] = kafkaDStream.map(tuple => tuple._2)

    //TODO 对接收每批次流式数据，进行词频统计WordCount
    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd =>
      rdd.filter(line => line != null && line.length > 0))
      .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
      .mapPartitions(iter => iter.map(word => (word, 1)))
      .reduceByKey((a, b) => a + b)

    wordCountDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("============================")
      println(s"Time:$batchTime")
      println("============================")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition(iter =>
            iter.foreach(item => println(item)))
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
