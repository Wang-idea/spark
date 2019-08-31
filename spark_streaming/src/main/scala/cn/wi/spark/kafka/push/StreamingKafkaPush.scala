package cn.wi.spark.kafka.push

import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingKafkaPush
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/31 13:34
 */
object StreamingKafkaPush {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      val context = new StreamingContext(sparkConf, Seconds(5))
      context.sparkContext.setLogLevel("WARN")
      context
    }

    /**
     * def createStream(
     * ssc: StreamingContext,
     * zkQuorum: String,
     * groupId: String,
     * topics: Map[String, Int],
     * storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     * ): ReceiverInputDStream[(String, String)]
     */

    //Zookeeper集群地址信息
    val zkQuorum: String = "node01:2181,node02:2181,node03:2181"

    //消费者所在组的Id
    val groupId: String = "group-id-00001"

    //设置消费Topic名称及相应分区数目，Map of (topic_name to numPartitions) to consume
    //topic:testTopic  分区数：3个
    val topics: Map[String, Int] = Map("testTopic" -> 3)

    // 采用Receiver方式读取Kafka Topic中的数据， 在Topic中每条数据，由Key和Value组成，Value表示Message信息
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      zkQuorum,
      groupId,
      topics,
      storageLevel = StorageLevel.MEMORY_AND_DISK
    )

    //获取从kafka读取数据的Message  kafka存储的数据是KV形式  所以取第二个值
    val inputDStream: DStream[String] = kafkaDStream.map(pair => pair._2)

    //TODO 对接收每批次流式数据，进行词频统计WordCount
    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd =>
      rdd.filter(line => line != null && line.length > 0)
        .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
        .mapPartitions(iter => iter.map(word => (word, 1)))
        .reduceByKey((a, b) => a + b))

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
