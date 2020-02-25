package xyz.myself.up.base

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import xyz.myself.up.bean.HBaseMeta

/**
  * Author lps
  * Date 2019/12/7
  * Desc trait相当于Java中的接口
  *
  */
trait BaseModel {
  //0.封装参数
  val config: Config = ConfigFactory.load()
  val url: String = config.getString("jdbc.url")
  val tableName: String = config.getString("jdbc.table")
  val sourceClass: String = config.getString("hbase.source.class")
  val zkHosts: String = config.getString("hbase.source.zkHosts")
  val zkPort: String = config.getString("hbase.source.zkPort")
  val hbaseTable: String = config.getString("hbase.source.hbaseTable")
  val family: String = config.getString("hbase.source.family")
  val selectFields: String = config.getString("hbase.source.selectFields")
  val rowKey: String = config.getString("hbase.source.rowKey")

  val hbaseMeta = HBaseMeta(
    "",
    zkHosts,
    zkPort,
    hbaseTable,
    family,
    selectFields,
    rowKey
  )

  // 0.创建sparkSession
  val spark: SparkSession = SparkSession.builder().master("local[*]")
    .appName("Model")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")
  // 导入隐式转换
  import spark.implicits._
  import  org.apache.spark.sql.functions._
  /**
    * 该抽象方法应该由子类实现
    * @return
    */
  def getTagId(): Long


  /**
    * 1.查询mysql中tbl_basic_tag的数据
    * @return
    */
  def getMySQLData(): DataFrame = {
    val url:String = "jdbc:mysql://192.168.10.20:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName:String = "tbl_basic_tag"
    val properties:Properties = new Properties()
    spark.read.jdbc(url,tableName,properties)
  }

  /**
    * 2.根据4级标签id查询mysql中的数据
    * @return
    */
  def getFourRule(mysqlDF: DataFrame): HBaseMeta = {
    val fourDF: Dataset[Row] = mysqlDF.select('rule).where('id===getTagId())
    val fourMap: Map[String, String] = fourDF.map(row => {
      val ruleStr: String = row.getAs[String]("rule")
      val kvs: Array[String] = ruleStr.split("##")
      kvs.map(kv => {
        val kvArr: Array[String] = kv.split("=")
        (kvArr(0), kvArr(1))
      })

    }).collectAsList().get(0).toMap
    HBaseMeta(fourMap)
  }

  /**
    * 3.根据4级标签的‘Id作为5级标签的父id
    * @param mysqlDF
    * @return
    */
  def getFiveDF(mysqlDF: DataFrame): DataFrame = {
    mysqlDF.select('id,'rule).where('pid===getTagId())

  }

  /**
    * 4.根据hbaseMeta查询HBase数据
    * @param hbaseMeta
    * @return
    */
  def getHBaseDF(hbaseMeta: HBaseMeta): DataFrame = {
    spark.read
      .format(sourceClass)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .load()
  }

  /**
    *5.根据5级标签和HBase数据进行标签计算(每个模型/标签计算方式不同)
    * 该抽象方法应该由子类/实现类提供具体的实现/扩展
    *
    * @param fiveDF
    * @param hbaseDF
    * @return
    */
  def computer(fiveDF:DataFrame,hbaseDF:DataFrame):DataFrame

  /**
    * 6.将新旧结果集进行合并
    * @param newDF
    * @return
    */
  def meger(newDF: DataFrame): DataFrame = {
    val oldDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.SELECTFIELDS, "userId,tagsId")
      .option(HBaseMeta.INTYPE, hbaseMeta.inType)
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, "detail")
      .load()

    //8.将newDF和oldDF进行join--DSL风格
    val tempDF: DataFrame = newDF.join(oldDF,newDF.col("userId")===oldDF.col("userId"),"left")

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

    newDF.join(oldDF, newDF.col("userId") === oldDF.col("userId"), "left")
      .select(newDF.col("userId"),
        meger(newDF.col("tagsId"), oldDF.col("tagsId"))
          .as("tagsId"))

  }

  /**
    * 7.保存结果
    * @param resultDF
    */
  def save(resultDF: DataFrame): Unit = {
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

  def execute(): Unit ={

    // 1.读取mysql数据
    val mysqlDF:DataFrame = getMySQLData()
    // 2.读取4级标签并解析
    val hbaseMeta:HBaseMeta = getFourRule(mysqlDF)
    // 3.读取5级标签
    val fiveDF:DataFrame = getFiveDF(mysqlDF)
    // 4.根据4级标签读取Hbase数据
    val hbaseDF:DataFrame = getHBaseDF(hbaseMeta)
    // 5.根据5级标签和Hbase数据进行标签计算（每个模型/标签计算方式不同）
    val newDF: DataFrame = computer(fiveDF,hbaseDF)
    // 6.将新的结果和旧的结果进行合并
    val resultDF:DataFrame = meger(newDF)
    // 7.将最终的结果存入Hbase
    save(resultDF)
  }

}
