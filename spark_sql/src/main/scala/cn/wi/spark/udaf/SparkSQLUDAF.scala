package cn.wi.spark.udaf

import org.apache.spark.sql.SparkSession

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLUDAF
 * @Author: xianlawei
 * @Description: 在IDEA中读取Hive表的数据们进行分析处理
 * @date: 2019/8/29 17:03
 */
object SparkSQLUDAF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      //spark.sql.shuffle.partitions，该参数代表了shuffle read task的并行度，该值默认是200，自定义并行度  优化
      .config("spark.sql.shuffle.partitions", "3")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //TODO 注册UDAF函数
    spark.udf register("avg_val", AvgUDAF)

    //采用SQL方式读取Hive表中的数据
    spark.sql(
      """
        |select
        |   deptno,
        |   avg(sal) as sal_avg
        |from
        |    db_hive.emp
        |group by
        |   deptno
        |""".stripMargin).show(10,truncate = false)

    spark.close()
  }
}
