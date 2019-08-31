package cn.wi.spark.union

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingWordCountUnion
 * @Author: xianlawei
 * @Description: 从TCP Socket中读取数据，对每批次数据进行词频统计，将统计结果输出到控制台
 * @date: 2019/8/31 12:30
 */
object StreamingWordCountUnion {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = {
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      val context = new StreamingContext(sparkConf, Seconds(5))
      context
    }

    val inputDStream01: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)
    val inputDStream02: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9998)
    val inputDStream: DStream[String] = inputDStream01.union(inputDStream02)

    val wordCountDStream: DStream[(String, Int)] = inputDStream.flatMap(line => line.trim.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
    wordCountDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
