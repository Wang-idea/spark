package cn.wi.spark.rdd

import cn.wi.spark.sparksession.MovieRating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkRDDToDataSet
 * @Author: xianlawei
 * @Description: 将RDD转换为DataSet或者DataFrame
 * @date: 2019/8/27 21:00
 */
object SparkRDDToDataSet {
  def main(args: Array[String]): Unit = {
    //TODO  构建SparkSession 实例
    val spark: SparkSession = {
      val session: SparkSession = SparkSession.builder()
        .master("local[3]")
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .getOrCreate()
      session.sparkContext.setLogLevel("WARN")
      session
    }
    import spark.implicits._
    //TODO 读取数据  .rdd 以rdd格式保存数据
    val rawsRDD: RDD[String] = spark.read.textFile("D:\\Spark\\data\\input\\exampleDatas\\ml-100k\\u.data").rdd

    //采用反射方式将RDD转换为DataFrame
    /**
     * RDD[CaseClass]
     * RDD.toDF  RDD.toDS
     */
    /**
     * mapPartitions 对每一块数据进行处理
     * iter 每一块的数据
     * line 每一行数据
     */

    val mlsRDD: RDD[MovieRating] = rawsRDD.mapPartitions { iter =>
      iter.map { line =>
        val Array(userId, itemId, rating, timestamp) = line.trim.split("\\t")
        //将数组中的数据封装到MovieRating对象中
        MovieRating(userId, itemId, rating.toDouble, timestamp.toLong)
      }
    }
    //通过隐式转换
    val mlsDF: DataFrame = mlsRDD.toDF()
    mlsDF.printSchema()
    mlsDF.show(10)

    val mlsDS: Dataset[MovieRating] = mlsRDD.toDS()
    mlsDS.printSchema()
    mlsDS.show(10)

    println("===========================================================")

    //TODO 自定义Schema方式
    /**
     * RDD[Row]
     * Schema
     * StructType -> StructFiled
     * createDataFrame
     */

    val rowsRDD: RDD[Row] = rawsRDD.mapPartitions {
      iter =>
        iter.map { line =>
          val Array(userId, itemId, rating, timestamp) = line.trim.split("\\t")
          //ROW字段名和上面Array的字段名必须一致
          Row(userId, itemId, rating.toDouble, timestamp.toLong)
        }
    }

    //自定义Schema信息  userId, itemId, rating, timestamp
    val mlSchema: StructType = StructType(
      Array(
        StructField("userId", StringType, nullable = true),
        StructField("itemId", StringType, nullable = true),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      )
    )

    val ratingsDF: DataFrame = spark.createDataFrame(rowsRDD, mlSchema)
    ratingsDF.printSchema()
    //false表示将每列值全部显示  不进行截取
    ratingsDF.show(10, truncate = false)

    //TODO 将DataFrame转换为DataSet，需要指定强类型接口
    val ratingsDS: Dataset[MovieRating] = ratingsDF.as[MovieRating]
    ratingsDS.printSchema()
    ratingsDS.show(10, truncate = false)

    spark.stop()
  }
}
