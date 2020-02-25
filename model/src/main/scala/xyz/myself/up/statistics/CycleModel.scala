package xyz.myself.up.statistics

import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, functions}
import xyz.myself.up.base.BaseModel

/**
  * Desc 开发消费周期模型
  * 获取用户在平台最近一次的消费时间，获取长时间未消费的用户来刺激消费
  */
object CycleModel extends BaseModel{

  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
    * 该抽象方法应该由子类实现
    *
    * @return
    */
  override def getTagId(): Long = 23

  /**
    * 5.根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
    * 该抽象方法应该由子类/实现类提供具体的实现/扩展
    *
    * @param fiveDF 5级标签
    * @param hbaseDF 根据4级标签查询出来的HBase数据
    * @return
    */
  override def computer(fiveDF: DataFrame, hbaseDF: DataFrame): DataFrame = {
    fiveDF.show(10,false)
    fiveDF.printSchema()

    hbaseDF.show(10,false)
    hbaseDF.printSchema()
    /*
    +---------+----------+
    |memberId |finishTime|
    +---------+----------+
    |13823431 |1564415022|
      */
    import  spark.implicits._
    import  org.apache.spark.sql.functions._
    val tempDF: DataFrame = hbaseDF.groupBy('memberId.as("userId"))
      .agg(max('finishTime).as("maxFinishTime"))
    tempDF.show(10,false)
    tempDF.printSchema()

    // 2.计算最近一次消费时间距离今天的天数

    //date_sub(current_date(),30)将当前时间向前推30天
    //from_unixtime()将时间戳转换为日期
    //datediff(start,end)计算两个日期的时间差(天数)
    val timediffColume: Column = functions.datediff(date_sub(current_date(),30),from_unixtime('maxFinishTime.cast(LongType)))
    val tempDF2: DataFrame = tempDF.select('userId,timediffColume.as("days"))

    tempDF2.show(10,false)
    tempDF2.printSchema()

    val rangDF: DataFrame = fiveDF.map(row => {
      val id: Long = row.getAs[Long]("id")
      val rule: String = row.getAs[String]("rule")
      val arr: Array[String] = rule.split("-")
      (id, arr(0).toInt, arr(1).toInt)
    }).toDF("tagsId", "start", "end")

    // 4.将tempDF2和rangDF进行匹配
    val newDF: DataFrame = tempDF2.join(rangDF)
      .where(tempDF2.col("days").between(rangDF.col("start"), rangDF.col("end")))
    newDF.show(10,false)

    newDF
  }
}
