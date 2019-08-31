package cn.wi.spark.file

import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingSourceFile
 * @Author: xianlawei
 * @Description: 从TCP Socket 中读取数据，对每批次（时间为1秒）数据进行词频统计，将统计结果输出到控制台。
 * @date: 2019/8/30 15:27
 */
object StreamingSourceFile {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      val context = new StreamingContext(sparkConf, Seconds(5))
      context
    }

    //TODO 从流式数据源读取数据，此处TCP Socket读取数据
    /**
     * def textFileStream(directory: String): DStream[String]
     */
    val inputDStream: DStream[String] = ssc.textFileStream("file:///D:/Spark/data/input/wordcount")

    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd =>
      rdd.filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
        .mapPartitions(iter =>
          iter.map(word => (word, 1)))
        .reduceByKey((a, b) => a + b))

    wordCountDStream.foreachRDD((rdd, time) => {
      val batchTime: String = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("===============================")
      println(s"Time:$batchTime")
      println("===============================")
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
