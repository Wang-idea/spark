package cn.wi.spark.udf

import org.apache.spark.sql.SparkSession

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLUDF
 * @Author: xianlawei
 * @Description: 在IDEA中读取Hive表的数据，进行分析处理，自定义UDF函数使用
 * @date: 2019/8/28 22:52
 */
object SparkSQLUDF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions", "3")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //TODO UDF需求->将EMP雇员表中的ename字段的值，转换为小写字母
    //方式一：在SQL中使用
    spark.udf.register(
      //函数名称  在SQL中使用
      "lower_name",
      //匿名函数 将名字改为小写
      (name: String) => name.toLowerCase
    )

    //采用SQL方式读取Hive表中的数据
    spark.sql(
      """
        |select ename, lower_name(ename) as name from db_hive.emp
        |""".stripMargin).show(10, truncate = false)

    spark.close()
  }
}
