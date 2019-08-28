package cn.wi.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkWordCountSubmit
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/24 22:08
 */
object SparkWordCountSubmit {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: SparkWordCountSubmit <input> <output> ...........")
      System.exit(0)
    }

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkWordCountSubmit")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val inputRDD: RDD[String] = sc.textFile(args(0))
    val wordRDD: RDD[String] = inputRDD.flatMap(line => line.split("\\s+"))
    val tuplesRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
    val wordCountRDD: RDD[(String, Int)] = tuplesRDD.reduceByKey((a, b) => a + b)

    wordCountRDD.saveAsTextFile(s"/spark/output/ideawordcount/wc-output-${System.currentTimeMillis()}")
    sc.stop()
  }
}
