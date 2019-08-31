package cn.wi.spark.transform

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingWordCountTransform
 * @Author: xianlawei
 * @Description: 从TCP Socket中读取数据，对每批次(时间为1s)数据进行词频统计，将统计结果输出到控制台
 * @date: 2019/8/30 10:33
 */
object StreamingWordCountTransform {
  def main(args: Array[String]): Unit = {
    //TODO 构建流式上下文实例对象Streaming Context，用于读取流式数据和调度Batch Job执行

    val ssc: StreamingContext = {
      //创建SparkConf实例对象，设置Application相关信息
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      //创建StreamingContext实例，传递Batch Interval(时间间隔：划分流式数据)
      val context = new StreamingContext(sparkConf, Seconds(5))
      context.sparkContext.setLogLevel("WARN")
      context
    }

    //TODO 从流失数据源读取数据，此处TCP Socket读取数据
    /**
     * def socketTextStream(
     * hostname: String,
     * port: Int,
     * storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     * ): ReceiverInputDStream[String]
     */

    val inputDStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    //TODO 对接收每批次流式，进行词频统计WordCount
    /**
     * def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
     */
    //Transform表示对DStream中每批次数据RDD进行操作
    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd => {

      rdd.filter(line => line != null && line.trim.length > 0)
        //分割为单词
        .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
        //转换为二元组
        .mapPartitions(iter =>
          iter.map(word => (word, 1)))
        .reduceByKey((a, b) => a + b)
    })
    //TODO 在DStream中，能对RDD的操作的不要对DStream操作

    //TODO 将分析每批次结果数据输出，此处打印控制台
    wordCountDStream.print()

    //TODO 对于有流式应用来说，需要启动应用，正常情况下启动以后一直运行，知道程序异常终止或者人为干涉
    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
