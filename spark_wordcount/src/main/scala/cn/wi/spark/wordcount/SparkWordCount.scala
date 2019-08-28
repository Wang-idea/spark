package cn.wi.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkWordCount
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/24 21:33
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkContext实例对象 对象名为sc
    //创建SparkConf对象封装Application相关配置
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkWordCount")

    //构建SparkContext上下文实例对象
    val sc: SparkContext = new SparkContext(sparkConf)

    //设置日志级别
    sc.setLogLevel("WARN")

    //TODO 读取要处理的数据文件，封装到RDD集合中
    val inputRDD: RDD[String] = sc.textFile("/spark/file/wordcount.data")

    //TODO 调用集合RDD中函数处理分析数据
    //对集合中每条数据按照空格进行分割，并将数据扁平化flatten
    val wordsRDD: RDD[String] = inputRDD.flatMap(line => line.split("\\s+"))

    //将每个单词转换为二元组  表示每个单词出现一次
    val tuplesRDD: RDD[(String, Int)] = wordsRDD.map(word => (word, 1))

    //按照Key进行分组聚合(先局部聚合，再全局聚合)
    val wordCountRDD: RDD[(String, Int)] = tuplesRDD.reduceByKey((a, b) => a + b)

    //TODO 保存结果RDD到外部存储系统(HDFS)
    wordCountRDD.saveAsTextFile(s"/spark/output/ideawordcount/wc-output-${System.currentTimeMillis()}")

    wordCountRDD.foreach(println)

    //TODO 为了查看应用 线程休眠
    Thread.sleep(15000)

    //关闭资源
    sc.stop()
  }
}
