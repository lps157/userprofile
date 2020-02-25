package xyz.myself.up.statistics

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import xyz.myself.up.bean.HBaseMeta

/**
  * Author lps
  * Date 2019/12/7
  * Desc 
  */
object AgeModel {
  def main(args: Array[String]): Unit = {
    // 0.创建SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("GenderModel")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    // 导入隐式转换
    import spark.implicits._

    // 1.获取Mysql数据
    val url:String = "jdbc:mysql://192.168.10.20:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName:String = "tbl_basic_tag"
    val properties:Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url,tableName,properties)
    mysqlDF.show(10,false);

    // 2.获取4级标签，并解析
    val fourDF: Dataset[Row] = mysqlDF.select('rule).where('id===4)
    val fourMap: Map[String, String] = fourDF.map(row => {
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##")
      kvs.map(kv => {
        val kvArr: Array[String] = kv.split("=")
        (kvArr(0), kvArr(1))
      })

    }).collectAsList().get(0).toMap

    val hbaseMeta = HBaseMeta(fourMap)

    // 3.获取5级标签-FiveDF
    val fiveDF: Dataset[Row] = mysqlDF.select('id,'rule).where('pid===14)
    fiveDF.show(10,false)

    val hbaseDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      // HBaseMeta.SELECTFIELDS，获取HbaseMeta中的常量
      // hBaseMeta.selectFields, 获取hbaseMeta样例类对象的字段值
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .load()
    println("hbase中的数据")
    hbaseDF.show(10,false)
  }
}
