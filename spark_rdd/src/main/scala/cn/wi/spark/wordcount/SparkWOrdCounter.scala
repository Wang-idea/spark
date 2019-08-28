package cn.wi.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkWOrdCounter
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/25 21:06
 */
object SparkWOrdCounter {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      //getSimpleName  简写  stripSuffix后缀
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val inputRDD: RDD[String] = sc.textFile("D:\\Spark\\data\\input\\wordcount.data")

    val wordCountsRDD: RDD[(String, Int)] = inputRDD
      .flatMap(line => line.split("\\s+"))
      .filter(word => word.length > 0)
      //表示的是每个分区中的数据，封装在一个迭代器中
      .mapPartitions {
        //与map中的word => (word, 1)一样
        iter => iter.map(word => (word, 1))
        //a,b 表示前后的(word,count)的V
      }.reduceByKey((a, b) => a + b)

    wordCountsRDD.foreachPartition {
      data =>
        data.foreach {
          case (word, count) => println(s"$word:  $count:")
        }
    }
    Thread.sleep(10000)
    sc.stop()
  }
}
