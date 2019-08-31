package cn.wi.spark.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingWordCount
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/29 21:40
 */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    //TODO 构建流式上下文实例对象StreamingContext,用于读取流式的数据和调度Batch Job执行
    val ssc: StreamingContext = {
      //创建实例化SparkConf对象，设置Application对象
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      //创建StreamingContext实例，传递Batch Interval(时间间隔：划分流式数据)  5s
      val context: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
      context.sparkContext.setLogLevel("WARN")
      context
    }


    //TODO 从流式数据源读取数据，此处 TCP Socket读取数据
    /**
     * def socketTextStream(
     * hostname: String,
     * port: Int,
     * storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     * ): ReceiverInputDStream[String]
     */
    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    //TODO 对接收每批次流式数据，及逆行词频统计WordCount
    val wordCountStream: DStream[(String, Int)] = inputDStream.flatMap(line =>
      line.trim.split("\\s+")
    ).map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
    wordCountStream.print()
    5
    //TODO 对于流式应用来说，需要启动应用，正常情况下启动以后一直运行，直到程序异常终止或者人为干涉
    //启动接收器Receivers，作为Long Running Task(线程)运行再Executor
    ssc.start()
    //一直等待
    ssc.awaitTermination()

    //是否停止stopSparkContext  是否优雅的停止
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
