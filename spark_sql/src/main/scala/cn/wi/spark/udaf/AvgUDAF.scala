package cn.wi.spark.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

/**
 * @ProjectName: Spark_Parent 
 * @ClassName: AvgUDAF
 * @Author: xianlawei
 * @Description: 自定义UDAF函数，实现：求取各个部门平均工资
 * @date: 2019/8/29 17:09
 */
object AvgUDAF extends UserDefinedAggregateFunction {
  /**
   * 表示：输入数据的类型，此处就是输入sal工资字段的名称
   * 将输入的数据封装在一个Schema中  见SparkRDDToDataSet.scala
   */
  override def inputSchema: StructType = StructType(
    Array(StructField("sal", DoubleType, nullable = true))
  )

  /**
   * 表示函数聚合时中间临时变量的数据类型，封装在Schema中
   * sal_total  总工资之和
   * sal_count 员工数
   */
  override def bufferSchema: StructType = StructType(
    Array(StructField("sal_total", DoubleType, nullable = true),
      StructField("sal_count", IntegerType, nullable = true))
  )

  /**
   * 表示：最后函数返回类型
   */
  override def dataType: DataType = DoubleType

  /**
   * 函数唯一性
   */
  override def deterministic: Boolean = true

  /**
   * 对聚合中间临时变量进行初始化
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //update第一个参数是下标 即表示bufferSchema中数组的值   第二个值给初始化赋予的值
    //表示设置sal_total初始值为0.0
    //0是下标  0.0是初始值
    buffer.update(0, 0.0)
    //1下标表示的是sal_count
    buffer.update(1, 0)
  }

  /**
   * 表示对每个分区数据进行聚合操作
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //表示inputSchema中数组的下标   与inputSchema对应
    // 获取输入工资
    val inputSalary: Double = input.getDouble(0)

    //下标的序号与bufferSchema中数组的下标一一对应
    //获取中间临时变量的值
    val salTotal: Double = buffer.getDouble(0)
    val salCount: Int = buffer.getInt(1)

    //更新数据
    buffer.update(0, salTotal + inputSalary)
    buffer.update(1, salCount + 1)
  }

  /**
   * 表示将每个分区数据的结果进行聚合：全局聚合
   * buffer1 是可变的  将聚合后的数据存储到buffer1中
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //获取中间临时变量的值   即缓冲区中的值
    val salTotal1: Double = buffer1.getDouble(0)
    val salCount1: Int = buffer1.getInt(1)

    val salTotal2: Double = buffer2.getDouble(0)
    val salCount2: Int = buffer2.getInt(1)

    //合并及更新
    buffer1.update(0, salTotal1 + salTotal2)
    buffer1.update(1, salCount1 + salCount2)
  }

  /**
   * 表示，某个UDAF函数，最终结果数据  此处输出平均值
   */
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0) / buffer.getInt(1)
  }
}
