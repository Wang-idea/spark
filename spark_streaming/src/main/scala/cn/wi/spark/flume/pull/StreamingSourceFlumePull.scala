package cn.wi.spark.flume.pull

import java.net.InetSocketAddress
import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingSourceFlumePull
 * @Author: xianlawei
 * @Description: 集成Flume，采用Pull方式读取数据，对每批次（时间为1秒）数据进行词频统计，将统计结果输出到控制台。
 * @date: 2019/8/30 19:11
 */
object StreamingSourceFlumePull {
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
     * def createPollingStream(
     * ssc: StreamingContext,
     * addresses: Seq[InetSocketAddress],
     * storageLevel: StorageLevel
     * ): ReceiverInputDStream[SparkFlumeEvent]
     */
    val addresses: Seq[InetSocketAddress] = Seq(new InetSocketAddress("node03", 9999))

    val flumeDStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(
      ssc,
      addresses,
      StorageLevel.MEMORY_AND_DISK
    )

    val inputDStream: DStream[String] = flumeDStream.map(sparkFlumeEvent =>
      new String(sparkFlumeEvent.event.getBody.array()))

    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd => {
      rdd.filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
        .mapPartitions(iter => iter.map(word => (word, 1)))
        .reduceByKey((a, b) => a + b)
    })

    wordCountDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat
        .getInstance("yyyy/MM/dd HH:mm:ss").format(new Date(time.milliseconds))
      println("============================================")
      println(s"Time: $batchTime")
      println("============================================")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition(iter =>
            iter.foreach(item => println(item)))
      }
    }
    )

    ssc.start() // 启动接收器Receivers，作为Long Running Task（线程） 运行在Executor
    ssc.awaitTermination()

    // TODO: 6、结束Streaming应用执行
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
