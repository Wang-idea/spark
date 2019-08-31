package cn.wi.spark.checkpoint

import java.sql.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingOrderAmtTotalCkpt
 * @Author: xianlawei
 * @Description: 集成Kafka，采用Direct式读取数据，对每批次（时间为1秒）数据进行词频统计，将统计结果输出到控制台。
 *               仿双十一实时累加统计各个省份订单销售额。
 *               数据格式：订单ID,省份ID,订单金额
 *               orderId,provinceId,orderPrice
 * @date: 2019/8/31 17:23
 */
object StreamingOrderAmtTotalCheckPoint {

  //Streaming流式应用的检查点目录
  val CHECK_POINT_PATH: String = "/spark/checkpoint/ckpt-0004"

  def main(args: Array[String]): Unit = {
    /**
     * def getActiveOrCreate(
     * checkpointPath: String,
     * // 当流式应用第一次运行时（此时检查点目录不存在的时候），创建新的StreamingContext实例对象
     * creatingFunc: () => StreamingContext,
     * //下面的两个参数都有默认值 可以不传
     * hadoopConf: Configuration = SparkHadoopUtil.get.conf,
     * createOnError: Boolean = false
     * ): StreamingContext
     */
    val ssc: streaming.StreamingContext = StreamingContext.getActiveOrCreate(
      //先检查检查点目录头没有，如果存在，从检查点目录数据构建StreamingContext实例对象
      CHECK_POINT_PATH,
      // 当流式应用第一次运行时（此时检查点目录不存在的时候），创建新的StreamingContext实例对象
      () => {
        val sparkConf: SparkConf = new SparkConf()
          .setMaster("local[3]")
          .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
          //TODO: 设置每秒钟读取Kafka中Topic最大数据量
          .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        val context: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
        //设置检查点目录,通过目录时HDFS上目录，将数据存储在HDFS上  第一次需要设置检查点目录
        context.checkpoint(CHECK_POINT_PATH)

        // TODO 传递StreamingContext流式上下文实例对象，读取数据进行处理分析
        processStreamingData(context)
        context
      }
    )

    ssc.sparkContext.setLogLevel("WARN")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  /**
   * 抽象一个函数：专门从数据源读取流式数据，经过状态操作分析数据，最后将数据输出
   */
  def processStreamingData(ssc: StreamingContext): Unit = {
    //第一步  读数据
    val kafkaDStream: InputDStream[(String, String)] = {
      //表示从Kafka Topic读取数据的相关参数设置
      val kafkaParams: Map[String, String] = Map(
        "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
        "auto.offset.reset" -> "largest"
      )

      val topics: Set[String] = Set("orderTopic")

      // 采用Direct方式从Kafka 的Topic中读取数据
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, //
        kafkaParams, //
        topics
      )
    }

    val orderDStream: DStream[(Int, Double)] = kafkaDStream.transform(rdd =>
      rdd
        .filter(tuple => tuple._2 != null && tuple._2.trim.split(",").length >= 3)
        .mapPartitions(iter =>
          iter.map(message => {
            val Array(orderId, provinceId, orderPrice) = message._2.trim.split(",")
            //返回
            (provinceId.toInt, orderPrice.toDouble)
          }
          ))
        // TODO: 对每批次数据进行聚合操作 -> 当前批次中，每个省份销售订单额，优化
        .reduceByKey((a, b) => a + b)
    )

    val orderProvinceAmtDStream: DStream[(Int, Double)] = orderDStream.updateStateByKey(
      //values表示订单销售额  state：以前的状态
      (values: Seq[Double], state: Option[Double]) => {
        //统计当前批次中省份的总的订单销售额
        val currentOrderAmt: Double = values.sum

        //获取省份以前总的订单销售额  有就返回  没有就返回0
        val previousOrderAmt: Double = state.getOrElse(0.0)

        //计算最新省份订单销售额
        val orderAmt = currentOrderAmt + previousOrderAmt

        //返回最新订单总的销售额  Option要么返回NULL  要么值  有值的话  返回Some(值)
        //(Seq[V], Option[S]) => Option[S]  返回的是一个Option
        Some(orderAmt)
      }
    )

    orderProvinceAmtDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat
        .getInstance("yyyy/MM/dd HH:mm:ss")
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
  }
}
