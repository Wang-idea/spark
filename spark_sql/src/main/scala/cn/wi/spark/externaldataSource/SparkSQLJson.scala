package cn.wi.spark.externaldataSource

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLJson
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/28 19:27
 */
object SparkSQLJson {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    val jsonDF: DataFrame = spark.read.json("D:\\Spark\\data\\input\\exampleDatas\\json\\2015-03-01-17.json.gz")
    jsonDF.printSchema()
    jsonDF.show(10)

    println("========================================")
    val jsonDS: Dataset[String] = spark.read.textFile("D:\\Spark\\data\\input\\exampleDatas\\json\\2015-03-01-17.json.gz")
    jsonDS.printSchema()
    jsonDS.show(10)

    //使用函数，解析Json格式数据
    import org.apache.spark.sql.functions._
    jsonDS.select(
      get_json_object($"value","$.id").as("id"),
      get_json_object($"value","$.type").as("type"),
      get_json_object($"value","$.public").as("public"),
      get_json_object($"value","$.created_at").as("created_at")
    ).show(10,false)
    spark.close()
  }
}
