package cn.wi.spark.state

import java.sql.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, MapWithStateDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingOrderAmtState
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/31 16:41
 */
object StreamingOrderAmtState {
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
    ssc.checkpoint("/spark/checkpoint/ckpt-0006")

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
        // TODO 对每批次数据进行聚合操作 -> 当前批次中，每个省份销售订单额，优化
        .reduceByKey((a, b) => a + b)
    )

    // 实时累加统计各省份订单销售
    /**
     * def mapWithState[StateType: ClassTag, MappedType: ClassTag](
     * spec: StateSpec[K, V, StateType, MappedType]
     * ): MapWithStateDStream[K, V, StateType, MappedType]
     * 通过函数源码发现参数使用对象
     * StateSpec 实例对象
     * StateSpec
     * 表示对状态封装，里面涉及到相关数据类型
     * 如何构建StateSpec对象实例呢？？
     * StateSpec 伴生对象中function函数构建对象
     * def function[KeyType, ValueType, StateType, MappedType](
     *
     * 函数名称可知，针对每条数据更新Key的转态信息
     * mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
     */
    val spec: StateSpec[Int, Double, Double, (Int, Double)] = StateSpec.function(
      (provinceId: Int, orderAmtOption: Option[Double], state: State[Double]) => {
        //获取当前订单销售额
        val currentOrderAmt: Double = orderAmtOption.getOrElse(0.0)

        //从以前状态中获取省份对应订单销售额
        val previousOrderAmt: Double = state.getOption().getOrElse(0.0)

        //更新省份订单销售额
        val lastedOrderAmt: Double = currentOrderAmt + previousOrderAmt

        //更新状态
        state.update(lastedOrderAmt)

        //返回最新省份销售订单额
        (provinceId, lastedOrderAmt)
      }
    )

    //调用mapWithState函数进行实时累加状态统计
    val orderProvinceAmtDStream: DStream[(Int, Double)] = orderDStream.mapWithState(spec)

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
