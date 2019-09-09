package cn.wi.spark.grouping

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: MemberOrderInfo
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/9 13:17
 *       区域：城市
 *       会员类型：黄金会员等
 *       产品：一个月的黄金会员、三个月的黄金会员
 *       不同产品的价格
 */
case class MemberOrderInfo(area: String, memberType: String, product: String, price: Int)
