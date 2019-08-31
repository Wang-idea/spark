package cn.wi.spark.output

import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingOutputMySQL
 * @Author: xianlawei
 * @Description: 从TCP Socket中读取数据，对每批次(时间为1s) 数据进行词频统计，将统计结果输出到MySQL中
 * @date: 2019/8/30 14:49
 */
object StreamingOutputMySQL {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      val context = new StreamingContext(sparkConf, Seconds(5))
      context.sparkContext.setLogLevel("WARN")
      context
    }

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream(
      "node01",
      9999,
      storageLevel = StorageLevel.MEMORY_AND_DISK
    )

    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd =>
      rdd
        .filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
        .mapPartitions(iter => iter.map(word => (word, 1)))
        .reduceByKey((a, b) => a + b)
    )
    wordCountDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat
        .getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("=====================================")
      println(s"Time:$batchTime")
      println("=====================================")

      if (!rdd.isEmpty()) {
        rdd.coalesce(1)
          .foreachPartition(iter =>
            iter.foreach(item =>
              // 将每条数据插入到数据
              item))

        // 关闭连接(将连接放回连接池)
      }
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)

  }
}
