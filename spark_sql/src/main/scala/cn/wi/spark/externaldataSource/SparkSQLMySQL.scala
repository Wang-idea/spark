package cn.wi.spark.externaldataSource

import java.util.Properties

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLMySQL
 * @Author: xianlawei
 * @Description: SparkSQLMySQL直接MySQL表中读取数据
 * @date: 2019/8/28 19:44
 */
object SparkSQLMySQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = {
      val session: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        //设置聚合的分区数
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
      session.sparkContext.setLogLevel("WARN")
      session
    }
    import spark.implicits._
    //TODO 由于要分析数据，使用内置函数，导入SparkSQL函数库包

    /**
     * 读取MySQL表中数据
     */
    val url: String = "jdbc:mysql://node03:3306/test"
    val table: String = "so"

    val properties: Properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.jdbc.Driver")

    //TODO 方式一：单分区模式，读取RDBMS表的数据到DataFrame中，只有一个分区
    /**
     * def jdbc(url: String, table: String, properties: Properties): DataFrame
     */
    val jdbcDF: DataFrame = spark.read.jdbc(url, table, properties)
    jdbcDF.printSchema()
    jdbcDF.foreachPartition(iter =>
      //TaskContext.getPartitionId 分区Id
      println(s"p-${TaskContext.getPartitionId()}:${iter.size}"))
    println("================================================")

    // TODO 方式二：多分区，通过字段设置  容易造成数据倾斜 读取订单销售额大于20且小于200的订单数据，分区数目：5
    // 200到20差额180 5个分区 每个分区36个  从20至56 从56至92........
    /**
     * def jdbc(
     * url: String,
     * table: String,
     *
     * columnName: String, // 字段名称
     * lowerBound: Long, // 下限值
     * upperBound: Long, // 上限值
     * numPartitions: Int, // 分区数目
     *
     * connectionProperties: Properties
     * ): DataFrame
     */
    val jdbcDF2: DataFrame = spark.read.jdbc(
      url,
      table,
      "order_amt", //columnName
      20L, //lowerBound
      200L, //upperBound
      5, //numPartitions
      properties
    )
    jdbcDF2.printSchema()
    jdbcDF2.foreachPartition(iter => println(s"p-${TaskContext.getPartitionId()}:${iter.size}"))
    println("====================================================")

    //TODO 方式三：高度自由分区
    /**
     * def jdbc(
     * url: String,
     * table: String,
     * predicates: Array[String],
     * connectionProperties: Properties
     * ): DataFrame
     */
    //设置WHERE条件语句  手动设置范围  一个Where条件语句就是一个分区的数据
    val predicates: Array[String] = Array(
      "order_amt >= 20 AND order_amt < 30", "order_amt >= 30 AND order_amt < 40",
      "order_amt >= 40 AND order_amt < 55", "order_amt >= 55 AND order_amt < 80",
      "order_amt >= 80 AND order_amt < 200"
    )
    val jdbcDF3: DataFrame = spark.read.jdbc(
      url,
      table,
      predicates,
      properties
    )
    jdbcDF3.printSchema()
    jdbcDF3.foreachPartition(iter => println(s"p-${TaskContext.getPartitionId()}:${iter.size}"))
    spark.close()
  }
}
