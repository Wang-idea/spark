package cn.wi.spark.clickdemon

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkClickStreamingLog
 * @Author: xianlawei
 * @Description: 使用Spark对点击流日志进行分析，统计PV、UV和TopKey Refer进行分析
 *               PV（Page View）访问量, 即页面浏览量或点击量
 *               UV（Unique Visitor）独立访客，统计1天内访问某站点的用户数(以cookie为依据);
 *               Refer：外链
 * @date: 2019/8/26 16:07
 */
object SparkClickStreamingLog {
  def main(args: Array[String]): Unit = {
    //TODO 构建SparkContext实例对象
    //这种方式
    val sc: SparkContext = {
      //设置应用配置信息
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      //TODO
      //建议使用getOrCreate创建获取SparkContext上下文实例对象
      SparkContext.getOrCreate(sparkConf)
    }
    sc.setLogLevel("WARN")

    //TODO 读取点击流日志数据，从本地文件系统中读取，设置最小分区数目  minPartitions
    //三个分区
    val accessLogsRDD: RDD[String] = sc.textFile("D:\\Spark\\data\\input\\logs\\access.log", minPartitions = 3)

    //TODO 数据ETL -> 过滤脏数据、提取字段(IP、URL和Refer)
    val filterRDD: RDD[(String, String, String)] = accessLogsRDD
      //过滤空数据和分割以后长度小于11的日志数据
      .filter(log => null != log && log.trim.split("\\s+").length >= 11)
      //针对每个分区进行操作
      .mapPartitions { iter =>
        iter.map { log =>
          val arr: Array[String] = log.trim.split("\\s+")
          //提取字段  返回一个三元组
          (arr(0), arr(6), arr(10))
          //过滤条件URL非空
        }.filter(tuple => tuple._2 != null && tuple._2.trim.length > 0)
      }

    //对上述ETL后RDD使用多次，考虑缓存到RDD
    //MEMORY_AND_DISK:将数据先缓存到内存，内存不足缓存到磁盘，考虑副本数和序列化
    filterRDD.persist(StorageLevel.MEMORY_AND_DISK)
    println(s"Filter Count = ${filterRDD.count()}")

    //TODO 业务一：PV统计
    //PV的URL不能为空   tuple._2.trim.length > 0已经将URL为空的过滤掉了  所以filterRDD的数目就是PV的数量
    val totalPV: Long = filterRDD.count()
    println(s"PV = $totalPV")

    //TODO 业务二：UV统计
    val totalUV: Long = filterRDD.mapPartitions { data
    =>
      //提取IP字段
      data.map(tuple => tuple._1)
    }.distinct().count() //统计IP地址出现次数，也就是UV

    println(s"UV=$totalUV")

    //TODO 业务三：TopKey Refer
    val top10: Array[(String, Int)] = filterRDD
      //提取出字段IP地址
      .mapPartitions {
        data => data.map(tuple => (tuple._3, 1))
        //a,b 表示前后的(word,count)的V   按外链Refer字段分组聚合
      }.reduceByKey((a, b) => a + b)
      //对次数进行排序
      .sortBy(tuple => tuple._2, ascending = false)
      .take(10)
    top10.foreach(println)

    //当缓存RDD不再使用，释放资源
    filterRDD.unpersist()

    sc.stop()
  }

}
