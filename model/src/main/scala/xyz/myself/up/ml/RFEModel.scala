package xyz.myself.up.ml

import org.apache.spark.sql.DataFrame
import xyz.myself.up.base.BaseModel

/**
  * Author lps
  * Date 2019/12/10
  * Desc  使用RFE客户活跃度模型 + KMeans模型
  * R：最近一次访问时间距离今天的天数
  * F：最近一段时间的访问频率
  * E：最近一段时间的页面互动率：浏览数/点击数
  */
object RFEModel extends BaseModel{
  /**
    * 该抽象方法应该由子类实现
    *
    * @return
    */
  override def getTagId(): Long = 45

  /**
    *5.根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
    * 该抽象方法应该由子类/实现类提供具体的实现/扩展
    *
    * @param fiveDF
    * @param hbaseDF
    * @return
    */
  override def computer(fiveDF: DataFrame, hbaseDF: DataFrame): DataFrame = {
    fiveDF.show(10,false)


  }

  def main(args: Array[String]): Unit = {

  }
}
