package cn.wi.spark.output

import java.sql.Date

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: StreamingOutput
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/30 11:12
 */
object StreamingOutput {
  def main(args: Array[String]): Unit = {
    //TODO 构建流式上下文实例对象StreamingContext，用于读取流式的数据和调度Batch Job执行
    val ssc: StreamingContext = {
      //创建SparkConf实例对象，设置Application相关信息
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      val context = new StreamingContext(sparkConf, Seconds(5))
      context.sparkContext.setLogLevel("WARN")
      context
    }

    //TODO 从流式数据源读取数据，此处TCP Socket读取数据
    /**
     * def socketTextStream(
     * hostname: String,
     * port: Int,
     * storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     * ): ReceiverInputDStream[String]
     */
    val inputDStream: ReceiverInputDStream[String] = ssc
      .socketTextStream(
        "node01",
        9999,
        storageLevel = StorageLevel.MEMORY_AND_DISK)

    //TODO 对接收每批次流式数据，进行词频统计WordCount
    /**
     * def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
     */
    //transform标志对DStream中每批次数据RDD进行操作
    val wordCountDStream: DStream[(String, Int)] = inputDStream.transform(rdd =>
      rdd.filter(line => line != null && line.trim.length > 0)
        .flatMap(line => line.split("\\s+").filter(word => word.length > 0))
        .mapPartitions(iter => iter.map(word => (word, 1)))
        .reduceByKey((a, b) => a + b))

    //TODO 将分析每批次结果数据输出，此处答应控制台
    wordCountDStream.print()

    /**
     * def foreachRDD(foreachFunc: (RDD[T], Time) => Unit): Unit
     * 其中Time就是每批次BatchTime，Long类型数据, 转换格式：2019/08/28 16:53:25
     */

    //TODO 对DStream中每批次结果RDD数据进行输出操作
    wordCountDStream.foreachRDD((rdd, time) =>
      // 使用lang3包下FastDateFormat日期格式类，属于线程安全的
    {
      //将Long类型的时间 转换成String类型
      val batchTime: String = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
        .format(new Date(time.milliseconds))
      println("==========================================")
      println(s"Time:$batchTime")
      println("============================================")

      //TODO 先判断RDD是否有数据，有数据再输出哦
      if (!rdd.isEmpty()) {
        //结果RDD输出 需要考虑降低分区数目
        rdd.coalesce(1)
          .foreachPartition(iter =>
            iter.foreach(item => println(item)))
      }
    }
    )

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }
}
