package com.xiaoyu.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object SparkUtil {

  /**
   * spark 的环境配置对象
   */
   private var conf: SparkConf = _

  /**
   * 获取公共的配置
   * @return prop
   */
 private def getProp: Properties = {
   val prop = new Properties()
   prop.put("driver", "com.mysql.jdbc.Driver")
   prop.put("user", "root")
   prop.put("password", "123456")
   prop
 }

  /**
   * 获取支持hive的sparkSession
   * @return 返回支持hive的sparkSession
   */
  def getHiveSparkSession(isCluster: Boolean, appName: String = "task"): SparkSession = {
    if (isCluster) {
      conf = new SparkConf().setAppName("task")
    } else {
      conf = new SparkConf().setMaster("local[*]").setAppName(appName)
    }
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  /**
   * 获取普通的sparkSession
   * @return 返回sparkSession
   */
  def getSparkSession(isCluster: Boolean, appName: String = "task"): SparkSession = {
    if (isCluster) {
      conf = new SparkConf().setAppName("task")
    } else {
      conf = new SparkConf().setMaster("local[*]").setAppName(appName)
    }
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }


  /**
   * 将df中的数据保存到mysql(覆写模式)
   * @param dataFrame df
   * @param database 需要操作的数据库
   * @param tableName 需要操作的表
   * @return 返回DataFormat
   */
  def saveMysql(dataFrame: DataFrame, database: String, tableName: String): Unit = {
    dataFrame.write.mode("overwrite")
      .jdbc( s"jdbc:mysql://172.29.44.24:3306/$database?useSSL=false&createDatabaseIfNotExist=true", tableName, getProp)
  }


  /**
   * 读取mysql中的数据
   * @param database  需要操作的数据库
   * @param tableName 需要操作的表
   * @return
   */
  def readMysql(spark: SparkSession, database: String, tableName: String): DataFrame = {
    spark.read.jdbc(s"jdbc:mysql://172.29.44.24:3306/$database?useSSL=false", tableName, getProp)
  }


  /**
   * 静态全量抽取
   * @param spark          sparkSession
   * @param hiveDb         需要操作的hive库名
   * @param hiveTableName  hive表名
   * @param tempTable      spark中的临时表
   * @param partitionFiled 分区字段
   * @param partitionVal   分区名
   */
  def saveHive(spark: SparkSession, hiveDb: String, hiveTableName: String, tempTable: String, partitionFiled: String, partitionVal: String): Unit = {
    spark.sql(s"use $hiveDb")
    spark.sql(
      s"""
         |insert overwrite table $hiveDb.$hiveTableName partition ($partitionFiled=$partitionVal)
         |select * from $tempTable
         |""".stripMargin)
  }


}
