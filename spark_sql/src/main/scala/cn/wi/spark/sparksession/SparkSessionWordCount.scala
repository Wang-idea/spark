package cn.wi.spark.sparksession

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSessionWordCount
 * @Author: xianlawei
 * @Description: 使用SparkSession读取文本数据，进行词频统计
 * @date: 2019/8/27 19:29
 */
object SparkSessionWordCount {
  def main(args: Array[String]): Unit = {
    //TODO 采用建造者模式，构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.eventLog.enable", "true")
      .config("spark.eventLog.dir", "hdfs//node01:8020/spark/eventLogs")
      .config("spark.eventLog.compress", "true")
      //此函数表示，当存在即返回，不存在时创建新的对象，单例模式，底层还是SparkContext实例对象
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //TODO For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    //TODO 读取HDFS上数据 DataSet=RDD+Schema（字段名称和字段类型）
    val inputDS: Dataset[String] = spark.read.textFile("/spark/file/wordcount.data")

    //TODO 答应Schema信息
    inputDS.printSchema()
    inputDS.show(10)

    /**
     * Schema信息
     * root
     * |-- value: string (nullable = true)
     */

    //TODO 词频统计 DataFrame = Dataset[Row]
    val wordCountDF: DataFrame = inputDS.flatMap(line => line.split("\\s+").filter(word => word != null && word.length > 0))
      .groupBy("value").count()
    wordCountDF.printSchema()
    wordCountDF.show(10)

    Thread.sleep(10000)
    spark.stop()
  }
}
