package cn.wi.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkWordCountTopKey
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/24 22:08
 */
object SparkWordCountTopKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkWordCountTopKey")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val inputRDD: RDD[String] = sc.textFile("/spark/file/wordcount.data")

    val wordsRDD: RDD[String] = inputRDD.flatMap(line => line.split("\\s+"))
    val tuplesRDD: RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
    val wordCountsRDD: RDD[(String, Int)] = tuplesRDD.reduceByKey((a, b) => a + b)

    wordCountsRDD.foreach(println)

    // 按照词频进行降序排序 获取次数最多的三个单词
    //TODO 方式一：sortByKey按照Key今次那个排序，建议使用此种当时排序
    // TODO swap:将元组的K V交换 K值变成V V变成K  将自身的升序排序关掉
    /**
     * def sortByKey(
     * ascending: Boolean = true,
     * numPartitions: Int = self.partitions.length
     * ): RDD[(K, V)]
     */
    wordCountsRDD.map(tuple => tuple.swap).sortByKey(ascending = false).take(3).foreach(println)

    //将交换的结果再交换回来
    // wordCountsRDD.map(tuple => tuple.swap).sortByKey(ascending = false).take(3).map(tuple => tuple.swap)foreach(println)

    //TODO 方式二： sortBy按照Key进行排序，底层还是sortByKey函数
    /**
     * def sortBy[K](
     * f: (T) => K, // 指定如何排序，此处设置按照二元组Value排序
     * ascending: Boolean = true,
     * numPartitions: Int = this.partitions.length
     * )
     * (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
     */
    wordCountsRDD.sortBy(tuple => tuple._2, ascending = false).take(3).foreach(println)

    //TODO 方式三：top函数慎用
    /**
     * def top(num: Int)(implicit ord: Ordering[T]): Array[T]
     */
    wordCountsRDD.top(3)(Ordering.by(tuple => tuple._2)).foreach(println)

    Thread.sleep(100000)
    sc.stop()
  }
}
