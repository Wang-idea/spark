package cn.wi.spark.grouping

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: SparkSQLGroup
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/9 13:17
 */
object SparkSQLGroupBy {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val orders: Seq[MemberOrderInfo] = Seq(
      MemberOrderInfo("深圳", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("深圳", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("深圳", "钻石会员", "钻石会员3个月", 70),
      MemberOrderInfo("深圳", "钻石会员", "钻石会员12个月", 300),
      MemberOrderInfo("深圳", "铂金会员", "铂金会员3个月", 60),
      MemberOrderInfo("深圳", "铂金会员", "铂金会员3个月", 60),
      MemberOrderInfo("深圳", "铂金会员", "铂金会员6个月", 120),
      MemberOrderInfo("深圳", "黄金会员", "黄金会员1个月", 15),
      MemberOrderInfo("深圳", "黄金会员", "黄金会员1个月", 15),
      MemberOrderInfo("深圳", "黄金会员", "黄金会员3个月", 45),
      MemberOrderInfo("深圳", "黄金会员", "黄金会员12个月", 180),
      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("北京", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("北京", "铂金会员", "铂金会员3个月", 60),
      MemberOrderInfo("北京", "黄金会员", "黄金会员3个月", 45),
      MemberOrderInfo("上海", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("上海", "钻石会员", "钻石会员1个月", 25),
      MemberOrderInfo("上海", "铂金会员", "铂金会员3个月", 60),
      MemberOrderInfo("上海", "黄金会员", "黄金会员3个月", 45)
    )

    val dataDS: Dataset[MemberOrderInfo] = orders.toDS()
    dataDS.printSchema()
    dataDS.show(5, truncate = false)

    println("===========================================")

    // 后续使用SQL分析，所以将Dataset注册为临时视图
    dataDS.createOrReplaceTempView("view_tmp_orders")

    //TODO group by 分组统计
    /**
     * 统计总订单金额
     * 统计各个区域订单金额
     * 统计各个区域各种会员类型订单金额
     * 统计各个区域各种会员类型购物不同产品的订单金额
     */

    println("==================统计总订单金额=====================")
    spark.sql(
      """
        |select
        |   sum(price) as sum_price
        |from
        | view_tmp_orders
        |""".stripMargin)
      .show(50, truncate = false)

    println("===============统计各个区域订单金额======================")
    spark.sql(
      """
        |select
        |     area,sum(price) as sum_price
        |from
        |     view_tmp_orders
        |group by
        |      area
        |order by
        |       area asc
        |""".stripMargin)
      .show(50, truncate = false)

    println("===============统计各个区域各种会员类型订单金额======================")

    spark.sql(
      """
        | select
        |   area,memberType,sum(price) as sum_price
        | from
        |    view_tmp_orders
        |  group by
        |     area ,memberType
        |   order by
        |     area asc,memberType asc
        |""".stripMargin)
      .show(50, truncate = false)


    println("=================统计各个区域各种会员类型购物不同产品的订单金额=====================")
    spark.sql(
      """
        | select
        |     area,memberType,product,sum(price) as sum_price
        | from
        |     view_tmp_orders
        |  group by
        |     area,memberType,product
        |  order by
        |     area asc,memberType asc,product asc
        |""".stripMargin)
      .show(50, truncate = false)
  }
}
