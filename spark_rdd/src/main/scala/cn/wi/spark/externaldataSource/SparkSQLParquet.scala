package cn.wi.spark.externaldataSource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLParquet
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/27 22:05
 */
object SparkSQLParquet {
  def main(args: Array[String]): Unit = {
    //TODO 构建SparkSession实例
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //TODO 读取Parquet格式文件数据，从本地文件系统读取
    val defaultDF: DataFrame = spark.read.load("D:\\Spark\\data\\input\\resources\\users.parquet")
    defaultDF.printSchema()
    defaultDF.show(10, truncate = false)

    println("=====================================")
    val parquetDF: DataFrame = spark.read.parquet("D:\\Spark\\data\\input\\resources\\users.parquet")
    parquetDF.printSchema()
    parquetDF.show(10,truncate = false)

    spark.stop()
  }
}
