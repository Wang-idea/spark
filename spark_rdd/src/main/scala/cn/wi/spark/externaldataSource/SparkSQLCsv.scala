package cn.wi.spark.externaldataSource

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLCsv
 * @Author: xianlawei
 * @Description: SparkSQL读取CSV格式据
 * @date: 2019/8/27 22:35
 */
object SparkSQLCsv {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = {
      val session: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .getOrCreate()
      session.sparkContext.setLogLevel("WARN")
      session
    }
    import spark.implicits._

    //读取csv格式文件数据，从本地文件系统读取
    //TODO 当读取csv格式数据时，如果文件首行不是列名称，自定Schema信息
    val schema: StructType = StructType(
      Array(
        StructField("userId", StringType, nullable = true),
        StructField("itemId", StringType, nullable = true),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )

    val csvDF: DataFrame = spark.read
      .schema(schema)
      // 表示每行数据中各个字段之间的分隔符（必须是单字符），默认值为逗号，可以进行设置
      .option("sep", "\\t")
      .csv("D:\\Spark\\data\\input\\exampleDatas\\ml-100k\\u.data")
    csvDF.printSchema()
    csvDF.show(10, truncate = false)

    println("=================================================")

    //TODO 读取CSv文件数据，首行是列的名称
    val csvHDF: DataFrame = spark.read
      .option("sep", "\\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\Spark\\data\\input\\exampleDatas\\ml-100k\\u.dat")
    csvHDF.printSchema()
    csvHDF.show(10, truncate = false)

    println("==========================================================")
    //TODO 降低分区数为1，此时保存JSON文件就是一个文件
    csvHDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json("D:\\Spark\\data\\output\\sparksqlcsv\\")


    spark.read
      .option("sep", "\\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:\\Spark\\data\\input\\exampleDatas\\ml-100k\\u.dat")
      //设置分区数
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .json("D:\\Spark\\data\\output\\sparksqlcsv\\")
    spark.close()
  }
}
