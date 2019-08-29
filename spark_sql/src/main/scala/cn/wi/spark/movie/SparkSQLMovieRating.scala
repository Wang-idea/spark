package cn.wi.spark.movie

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLMovieRating
 * @Author: xianlawei
 * @Description:
 * @date: 2019/8/28 21:18
 */
object SparkSQLMovieRating {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //TODO  读取电影评分数据，从本地文件系统读取
    //数据格式： 1::1193::5::978300760
    val ratingDS: Dataset[String] = spark
      .read.textFile("D:\\Spark\\data\\input\\exampleDatas\\ml-1m\\ratings.dat")

    //TODO 对读取的数据进行转换操作，将ratingDS转换成DataFrame -> (UserID::MovieID::Rating::Timestamp)
    val ratingsDS: Dataset[MovieRating] = ratingDS.mapPartitions(iter =>
      iter.map(line => {
        val Array(userId, movieId, rating, timestamp) = line.trim.split("::")
        MovieRating(userId, movieId, rating.toDouble, timestamp.toLong)
      }))

    //TODO 使用SQL分析
    //注册DataSet为临时视图
    ratingsDS.createOrReplaceTempView("view_ratings")
    //需求：对电影评分数据进行统计分析，
    // 获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)
    //2 小数点后两位
    spark.sql(
      """
        |select
        |movieId, round(avg(rating),2) as avg_rating,count(movieId) as movie_count
        |from view_ratings
        |group by movieId
        |having movie_count > 2000
        |order by movie_count desc,avg_rating desc
        |limit 10
        |""".stripMargin).show(10, truncate = false)

    println("=====================================")

    //TODO 使用DSL分析
    //选取字段的值， 将列名的名称（字符串）前面加上 $ 后，将字符串转换为Column对象（表示列）
    ratingsDS.select($"movieId", $"rating")
      //按照movieId进行分组
      .groupBy($"movieId")
      .agg(
        count($"movieId").as("movie_count"),
        round(avg($"rating"), 2).as("avg_rating")
      ).filter($"movie_count".gt(2000))
      .orderBy($"movie_count".desc, $"avg_rating".desc)
      .limit(10)
      .show(10, truncate = false)

    spark.close()
  }
}
