package cn.wi.spark.externaldataSource

import org.apache.spark.sql.SparkSession

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLHive
 * @Author: xianlawei
 * @Description: 读取Hive表的数据，进行分析处理
 * @date: 2019/8/28 21:01
 */
object SparkSQLHive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    spark.sql(
      """
        |select *from db_lianjia.tb_second limit 10
        |""".stripMargin)
      .show(10, truncate = false)

    spark.stop()
  }
}
