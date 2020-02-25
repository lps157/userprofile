package xyz.myself.up.matchtag

import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import xyz.myself.up.bean.HBaseMeta

object GenderModel {
  def main(args: Array[String]): Unit = {
    // 0.创建sparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("GenderModel")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    // 导入隐式转换
    import spark.implicits._

    // 1.读取MySQL数据--MySQLDF
    val url:String = "jdbc:mysql://192.168.10.20:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName:String = "tbl_basic_tag"
    val properties:Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url,tableName,properties)
    mysqlDF.show(10,false);
    // 2.读取mysqlDF中的4级标签
    val fourRuleDS: Dataset[Row] = mysqlDF.select("rule").where("id=4")
    println("mysqlDF中的4级标签：")
    fourRuleDS.show(10,false)

    //3.解析rule,根据解析出来的信息,去读取HBase数据--HBaseDF
    val fourRuleMap: Map[String, String] = fourRuleDS.map(row => {
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##")
      kvs.map(kv => {
        val tempKvs: Array[String] = kv.split("=")
        (tempKvs(0), tempKvs(1))
      })
    }).collectAsList().get(0).toMap
    println("rule解析后的结果")
    fourRuleMap.foreach(println)

    // 将fourRuleMap转换为方便后面使用的HbaseMeta样例类
    val hbaseMeta: HBaseMeta = HBaseMeta(fourRuleMap)

    // 4.读取Hbase数据--使用封装好的Hbase数据源来读取
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

    // 5.读取mysql中的5级标签--fiveDF
    val fiveDF: Dataset[Row] = mysqlDF.select('id,'rule).where('pid===4)
    println("mysql中的5级标签内容：")
    fiveDF.show(10,false)

    val fiveMap: Map[String, Long] = fiveDF.as[(Long, String)].map(t => {
      (t._2, t._1)
    }).collect().toMap

    // 定义DSL风格的udf
    import org.apache.spark.sql.functions._

    val gender2tag = udf((gender:String)=>{
      fiveMap(gender)
    })

    // 6.将HBaseDF和fiveDF进行关联，得到结果--resultDF
    val resultDF: DataFrame = hbaseDF.select('id.as("userId"),gender2tag('gender).as("tagsId"))

    resultDF.show(10,false)

    //7.将ResultDF存入到HBase标签结果表中
    resultDF.write
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.ROWKEY, "userId")
      .save()
    println("数据写入到HBase Success")
  }
}
