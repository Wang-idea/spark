package cn.wi.spark.output

import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingOutputHdfs
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/30 11:51
 */
object StreamingOutputHdfs {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        //设置数据输出文件系统的算法版本为2
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      val context = new StreamingContext(sparkConf, Seconds(5))
      context.sparkContext.setLogLevel("WARN")
      context
    }

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream(
      "node01",
      9999,
      storageLevel = StorageLevel.MEMORY_AND_DISK
    )

    inputDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("=================================")
      println(s"Time:$batchTime")
      println("===================================")

      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .saveAsTextFile(s"/spark/output/ideawordcount/${batchTime.substring(0, 10)}/logs-$time")
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
