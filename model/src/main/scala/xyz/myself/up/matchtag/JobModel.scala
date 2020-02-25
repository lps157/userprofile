package xyz.myself.up.matchtag

import java.util.Properties

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import xyz.myself.up.bean.HBaseMeta

/**
  * Author lps
  * Date 2019/12/5
  * Desc 
  */
object JobModel {
  def main(args: Array[String]): Unit = {
    // 0.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("jobModel")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import  org.apache.spark.sql.functions._

    // 1.读取mysql数据-mysqlDF
    val url:String = "jdbc:mysql://192.168.10.20:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName:String = "tbl_basic_tag"
    val properties:Properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url,tableName,properties)

    // 2.从mysqlDF获取4级标签规则
    val fourRuleDS: Dataset[Row] = mysqlDF.select('rule).where('id===7)
    println("4级标签规则:")
    fourRuleDS.show(10,false)

    // 3.从mysqlDF获取5级标签规则--fiveDF
    val fiveDF: Dataset[Row] = mysqlDF.select("id","rule").where("pid=7")
    println("5级标签规则")
    fiveDF.show(10,false)

    // 4.解析四级标签规则
    val fourRuleMap: Map[String, String] = fourRuleDS.map(row => {
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##")
      kvs.map(kv => {
        val kvArr: Array[String] = kv.split("=")
        (kvArr(0), kvArr(1))
      })
    }).collectAsList().get(0).toMap
    println("四级标签规则")
    fourRuleMap.foreach(println)

    val hbaseMeta: HBaseMeta = HBaseMeta(fourRuleMap)

    // 5.根据4级标签规则去HBase中查询数据--hbaseDF
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
    println("HBase中查询数据")
    hbaseDF.show(10,false)

    // 6.将hbaseDF和fiveDF进行关联，得到结果
    val fiveRuleMap: Map[String, Long] = fiveDF.as[(Long, String)].map(t => {
      (t._2, t._1)
    }).collect().toMap

    spark.udf.register("job2tag",(job:String)=>{
      fiveRuleMap(job)
    })

    hbaseDF.createOrReplaceTempView("t_hbase")
    val newDF: DataFrame = spark.sql("select id as userId,job2tag(job) as tagsId from t_hbase")
    println("newDF...")
    newDF.show(10,false)

    //注意:不能直接将newDF写入到HBase,因为这样会覆盖之前的数据
    //那么该如何做?
    //那么我们这里用一种简单的办法,将之前的结果查出来oldDF和现在的结果newDF进行合并,得到最终的结果resultDF,再写入到HBase
    // 7.查询oldDF
    val oldDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .load()
    oldDF.show(10,false)

    //8.将newDF和oldDF进行join--DSL风格
    val tempDF: DataFrame = newDF.join(oldDF,newDF.col("userId")===oldDF.col("userId"),"left")
    println("tempDF")
    tempDF.show(10,false)

    //自定义UDF--DSL风格
    val meger: UserDefinedFunction = udf((newTagsId: String, oldTagsId: String) => {
      if (StringUtils.isBlank(newTagsId)) {
        oldTagsId
      } else if (StringUtils.isBlank(oldTagsId)) {
        newTagsId
      } else {
        val arr1: Array[String] = newTagsId.split(",")
        val arr2: Array[String] = oldTagsId.split(",")
        val arr: Array[String] = arr1 ++ arr2
        arr.toSet.mkString(",")
      }
    })


   val tt: DataFrame = tempDF.select("")
   val value: Dataset[Row] = tt.join(oldDF,tt.col(" ")===oldDF.col(""),"left").select("").distinct()
    value.show()
    val resultDF: DataFrame = newDF.join(oldDF, newDF.col("userId") === oldDF.col("userId"), "left")
      .select(newDF.col("userId"),
        meger(newDF.col("tagsId"), oldDF.col("tagsId"))
          .as("tagsId"))
    resultDF.show(10,false)

    //9.将结果写入到HBase
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
