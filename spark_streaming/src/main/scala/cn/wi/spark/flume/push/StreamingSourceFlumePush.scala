package cn.wi.spark.flume.push

import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingSourceFlumePush
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/30 17:27
 */
object StreamingSourceFlumePush {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      val context = new StreamingContext(sparkConf, Seconds(5))

      context.sparkContext.setLogLevel("WARN")

      context
    }

    //TODO 从流式数据源读取数据，此处TCP Socket读取数据
    /**
     * def createStream (
     * ssc: StreamingContext,
     * hostname: String,
     * port: Int,
     * storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     * ): ReceiverInputDStream[SparkFlumeEvent]
     */
    val flumeDStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(
      ssc,
      "192.168.72.1",
      9999,
      storageLevel = StorageLevel.MEMORY_AND_DISK
    )

    val inputDStream: DStream[String] = flumeDStream.map(sparkFlumeEvent =>
      new String(sparkFlumeEvent.event.getBody.array()))

    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd =>
      rdd.filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
        .mapPartitions(iter => iter.map(word => (word, 1)))
        .reduceByKey((a, b) => a + b))

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
