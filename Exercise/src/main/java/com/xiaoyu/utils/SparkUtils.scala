package com.xiaoyu.utils

import com.ibm.icu.text.SimpleDateFormat
import com.ibm.icu.util.Calendar
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.{Date, Properties}


object SparkUtils {

  private var conf: SparkConf = _

  private def getProp: Properties = {
    val prop = new Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "123456")
    prop
  }


  def getHiveSparkSession(isCluster: Boolean, appName: String = "task"): SparkSession = {

    if (isCluster) {
      conf = new SparkConf().setAppName(appName)
    } else {
      conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    }
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("set hive.exec.dynamic.partition=ture")
    spark.sql("set hive.exec.dynamic.partition.mode=nostric")
    spark
  }



  def readMysql(spark: SparkSession, mysqlDb: String, mysqlTableName: String): DataFrame = {
    spark.read.jdbc(s"jdbc:mysql://172.29.44.24:3306/$mysqlDb?useSSL=false", mysqlTableName, getProp)
  }



  def writeMysql(dataFrame: DataFrame, mysqlDb: String, mysqlTableName: String): Unit = {
    dataFrame.write.mode("overwrite")
      .jdbc(s"jdbc:mysql://172.29.44.24:3306/$mysqlDb?useSSL=false&createDatabaseIfNotExist=true", mysqlTableName, getProp)
  }


  def getyyyyMMddDate: String  = {
    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    new SimpleDateFormat("yyyyMMdd").format(calendar)
  }


  def getHHmmssDate:String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
  }


}
