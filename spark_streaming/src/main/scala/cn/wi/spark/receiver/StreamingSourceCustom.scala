package cn.wi.spark.receiver

import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingSourceCustom
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/30 16:44
 */
object StreamingSourceCustom {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      val context = new StreamingContext(sparkConf, Seconds(5))

      context.sparkContext.setLogLevel("WARN")
      context
    }

    val inputDStream: ReceiverInputDStream[String] = ssc.receiverStream(
      new CustomReceiver("node01", 9999)
    )

    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd =>
      rdd.filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
        .mapPartitions(iter => iter.map(word => (word, 1)))
        .reduceByKey((a, b) => a + b)
    )

    wordCountDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("=====================================")
      println(s"Time:$batchTime")
      println("=======================================")
      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition(iter =>
            iter.foreach(item => println(item))
          )
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
