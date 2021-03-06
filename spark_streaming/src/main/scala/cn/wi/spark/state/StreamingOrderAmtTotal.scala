package cn.wi.spark.state

import java.sql.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingOrderAmtTotal
 * @Author: xianlawei
 * @Description: 继承Kafka，采用Direct式读取数据，对每批次（时间为1秒）数据进行词频统计，将统计结果输出到控制台。
 *               仿双十一实时累加统计各个省份订单销售额。
 *               数据格式：订单ID,省份ID,订单金额
 *               orderId,provinceId,orderPrice
 * @date: 2019/8/31 15:32
 */
object StreamingOrderAmtTotal {
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

    //设置检查点
    ssc.checkpoint("/spark/checkpoint/ckpt-0002")

    val kafkaParams: Map[String, String] = Map("bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      // 表示从Topic的各个分区的哪个偏移量开始消费数据，设置为最大的偏移量开始消费数据
      "auto.offset.reset" -> "largest")

    val topics: Set[String] = Set("orderTopic")

    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topics
    )

    //s使用transform函数也是对RDD操作，但是无需判断RDD是否有值，
    // 原因在于RDD操作属于Transformation(属于Lazy)需要Action触发
    val orderDStream: DStream[(Int, Double)] = kafkaDStream.transform(rdd =>
      rdd
        //判断KV中V是否为空，业务的length>3    业务数据：201710261645320001,12,45.00
        .filter(tuple => tuple._2 != null && tuple._2.trim.split(",").length >= 3)
        //提取字段信息  省份ID,订单金额，以二元组返回
        //updateStateByKey 依据Key更新状态信息，有很多重载方法，依据具体的业务需求使用这些函数
        //此处省份Id为K
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

    // 实时累加统计各省份订单销售
    /**
     * def updateStateByKey[S: ClassTag](
     * updateFunc: (Seq[V], Option[S]) => Option[S]
     * ): DStream[(K, S)]
     * 核心：
     * 按照key更新状态信息，针对应用按照省份ID实时更新订单销售额
     * 解释函数参数：
     * (Seq[V], Option[S])
     * 		  		- Seq[V]: 表示的是当前批次中所有Key的Value集合，
     * 针对应用来时，表示某个省份的所有的订单的销售额
     * 		  		- Option[S]：表示的是当前批次中Key的以前状态
     * 针对应用来说，表示某个省份以前的总的订单销售额，此时S泛型类型：Double类型
     *
     * Option[S]：表示的当前Key的最新状态，
     * 针对应用来说就是某个省份的最新的总的订单销售额
     */
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

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
