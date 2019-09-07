package cn.wi.spark.window

import java.sql.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingOrderCountWindow
 * @Author: xianlawei
 * @Description: 集成Kafka，采用Direct式读取数据，对每批次（时间为1秒）数据进行词频统计，将统计结果输出到控制台。
 *               每隔2秒统计最近4秒的各个省份订单数目
 *               数据格式：订单ID,省份ID,订单金额
 *               orderId,provinceId,orderPrice
 * @date: 2019/8/31 19:12
 */
object StreamingOrderCountWindow {

  //Streaming 应用BatchInterval
  val STREAMING_BATCH_INTERVAL: Int = 2

  //Streaming 应用窗口大小 时间是BatchInterval的整数倍
  val STREAMING_WINDOW_INTERVAL: Int = STREAMING_BATCH_INTERVAL * 2
  //滑动窗口
  val STREAMING_SLIDER_INTERVAL: Int = STREAMING_BATCH_INTERVAL * 1

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      // a. 创建SparkConf实例对象，设置Application相关信息
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        //TODO: 设置每秒钟读取Kafka中Topic最大数据量
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")
      // b. 创建StreamingContext实例，传递Batch Interval（时间间隔：划分流式数据）
      val context: StreamingContext = new StreamingContext(
        sparkConf, Seconds(STREAMING_BATCH_INTERVAL)
      )
      // 设置日志级别
      context.sparkContext.setLogLevel("WARN")
      // c. 返回上下文对象
      context
    }

    // 设置检查点目录,通过目录时HDFS上目录，将数据存储在HDFS上
    ssc.checkpoint("/spark/checkpoint/ckpt-0007")


    // TODO: 2、从流式数据源读取数据，此处TCP Socket读取数据
    // 表示从Kafka Topic读取数据时相关参数设置
    val kafkaParams: Map[String, String] = Map(
      "bootstrap.servers" ->
        "node01:9092,node02:9092,node03:9092",
      // 表示从Topic的各个分区的哪个偏移量开始消费数据，设置为最大的偏移量开始消费数据
      "auto.offset.reset" -> "largest"
    )
    // 从哪些Topic中读取数据
    val topics: Set[String] = Set("orderTopic")
    // 采用Direct方式从Kafka 的Topic中读取数据
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics
    )

    // TODO: 窗口统计，每隔4秒统计最近6秒的各个省份订单数目
    // def window(windowDuration: Duration, slideDuration: Duration): DStream[T]
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(
      Seconds(STREAMING_WINDOW_INTERVAL),
      Seconds(STREAMING_SLIDER_INTERVAL)
    )

    val orderProvinceAmtDStream: DStream[(Int, Long)] = windowDStream.transform((rdd: RDD[(String, String)]) =>
      rdd
        .filter((tuple: (String, String)) => tuple._2 != null && tuple._2.trim.split(",").length >= 3)
        .mapPartitions((iter: Iterator[(String, String)]) =>
          iter.map((message: (String, String)) => {
            val Array(orderId, provinceId, orderPrice) = message._2.trim.split(",")
            //
            (provinceId.toInt, 1L)
          }
          ))
        .reduceByKey((a: Long, b: Long) => a + b)
    )
    orderProvinceAmtDStream.foreachRDD((rdd: RDD[(Int, Long)], time: Time) => {
      val batchTime: String = FastDateFormat
        .getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("============================")
      println(s"Time:$batchTime")
      println("============================")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition((iter: Iterator[(Int, Long)]) =>
            iter.foreach((item: (Int, Long)) => println(item)))
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
