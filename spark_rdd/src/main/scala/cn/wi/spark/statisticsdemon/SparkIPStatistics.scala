package cn.wi.spark.statisticsdemon

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: IPStatistics
 * @Author: xianlawei
 * @Description:
 *  通过日志信息（运行商或者网站自己生成）和城市ip段信息来判断用户的ip段，统计热点经纬度
 * 	a. 加载IP地址信息库，获取起始IP地址和结束IP地址Long类型值及对应经度和维度
 * 	b. 读取日志数据，提取IP地址，将其转换为Long类型的值
 * 	c. 依据Ip地址Long类型值获取对应经度和维度 - 二分查找
 * 	d. 按照经度和维分组聚合统计出现的次数，并将结果保存到MySQL数据库中
 * @date: 2019/8/26 17:33
 */
object SparkIPStatistics {
  def main(args: Array[String]): Unit = {
    //TODO  构建SparkContext实例对象
    val sc: SparkContext = {
      //创建SparkConf，设置应用配置信息
      val sparkConf: SparkConf = new SparkConf()
        .setMaster("local[3]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      SparkContext.getOrCreate(sparkConf)
    }
    sc.setLogLevel("WARN")

    val infoRDD: RDD[(Long, Long, String, String)] = sc
      .textFile("D:\\Spark\\data\\input\\ips\\ip.txt", minPartitions = 2)
      .filter(line => line != null && line.trim.split("\\|").length == 15)
      .mapPartitions { iter =>
        iter.map {
          info =>
            val arr: Array[String] = info.trim.split("\\|")
            (arr(2).toLong, arr(3).toLong, arr(arr.length - 2), arr(arr.length - 1))
        }
      }

    // 将RDD数据转存到数组中(RDD数据量不大)
    // 返回包含此RDD中所有元素的数组。
    // 只有在结果数组预期很小时才应使用此方法,所有数据都加载到驱动程序的内存中。
    val infoArray: Array[(Long, Long, String, String)] = infoRDD.collect()
    //TODO 使用广播将变量将数组广播到Executor中
    val infoArrayBroadcast: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(infoArray)

    //TODO 读取日志数据，提取IP地址，将其转换为Long类型的值
    val ipLocationCountRDD: RDD[((String, String), Int)] = sc
      .textFile("D:\\Spark\\data\\input\\ips\\20090121000132.394251.http.format", minPartitions = 3)
      .filter(log => null != log && log.trim.split("\\|").length > 2)
      //针对RDD分区数据进行操作
      .mapPartitions { iter =>
        iter.map { log =>
          //获取Ip地址
          val ipStr: String = log.trim.split("\\|")(1)
          //将IP地址转换为Long类型值
          val ipLong: Long = IPToLong(ipStr)
          //依据IP地址Long类型值获取对应经度和纬度
          val searchIndex: Int = binarySearch(ipLong, infoArrayBroadcast.value)

          //直接返回索引
          searchIndex
        }
          //过滤索引为-1值，表示未找到对应经纬度数据
          .filter(index => index != -1)
          //依据索引到数字钟获取对应经纬度数据
          .map { index =>
            val (_, _, longitude, latitude) = infoArrayBroadcast.value(index)
            //返回经纬度信息
            ((longitude, latitude), 1)
          }
      }
      //按照经纬度和纬度分组聚合统计出现的次数
      .reduceByKey((a, b) => a + b)

    //TODO 将统计的数据保存到MySQL中
    ipLocationCountRDD.coalesce(1)
      .foreachPartition(iter => saveToMySQL(iter))

    sc.stop()
  }

  /**
   * 将IPV4格式值转换为Long类型值
   *
   * @param ip
   * @return
   */
  def IPToLong(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      //位运算符  移位  先计算ipNum << 8L 二进制移动八位
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    //返回
    ipNum
  }

  /**
   * 依据IP地址Long类型的值到IP地址信息库数组中查找，采用二分法
   *
   * @param ipLong      IP地址Long类型的值
   * @param ipInfoArray IP地址信息库数组
   * @return 数组下标索引，如果是-1表示未查到
   */
  def binarySearch(ipLong: Long, ipInfoArray: Array[(Long, Long, String, String)]): Int = {
    //定义起始和结束索引
    var startIndex: Int = 0
    var endIndex: Int = ipInfoArray.length - 1

    //使用while循环判断
    while (startIndex <= endIndex) {
      //获取中间middle索引及对应数组中的值
      val middleIndex: Int = startIndex + (endIndex - startIndex) / 2
      //获取数组中值
      val (startIP, endIP, _, _) = ipInfoArray(middleIndex)

      // TODO:  当IpLong在startIp和endIp中间时，直接返回middleIndex
      if (ipLong >= startIP && ipLong <= endIP) {
        return middleIndex
      }
      //TODO  当ipLong小于startIP时，继续从数组左边折半开始查找
      if (ipLong < startIP) {
        endIndex = middleIndex - 1
      }

      //TODO 当ipLong大于endIP时，继续从数组右边折半查找
      if (ipLong > endIP) {
        startIndex = middleIndex + 1
      }
    }
    //当查找不到  返回-1
    -1
  }

  def saveToMySQL(iter: Iterator[((String, String), Int)]):Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    var conn: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")

      /**
       * 向数据库结果表中插入数据，不能直接使用INSERT语句，
       * 当主键存在时，更新值；不存在时插入 -> upSert 语句
       */
        //插入更新  如果存在就更新  不存在就插入
      val sqlStr: String = "INSERT INTO tb_iplocation (longitude, latitude, total_count) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE total_count=VALUES(total_count)"

      preparedStatement = conn.prepareStatement(sqlStr)

      //将迭代器数据插入到表中
      iter.foreach {
        case ((longitude, latitude), count) =>
          println(s"经度=$longitude,纬度=$latitude,次数=$count")
          preparedStatement.setString(1, longitude)
          preparedStatement.setString(2, latitude)
          preparedStatement.setLong(3, count)

          //将其加入批次中
          preparedStatement.addBatch()
      }

      preparedStatement.executeBatch()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (preparedStatement != null) preparedStatement.close()
      if (conn != null) conn.close()
    }
  }
}
