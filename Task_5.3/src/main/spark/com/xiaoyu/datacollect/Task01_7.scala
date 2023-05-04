package com.xiaoyu.datacollect

import com.xiaoyu.utils.SparkUtil
import org.apache.spark.sql.SparkSession


/**
 * 动态增量抽取
 */
object Task01_7 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().master("local[*]").appName("任务一：动态增量抽取（7）").getOrCreate()

    // 需要操作的库名和表名
    val mysqlDb: String = "shtd_store"
    val hiveDb: String = "ods"
    val mysqlTableName: String = "ORDERS"
    val hiveTableName: String = "orders"

    SparkUtil.readMysql(spark, mysqlDb, mysqlTableName).createOrReplaceTempView("mysql_data")

    spark.sql(
      s"""
        | select
        |     m.*
        | from
        |     mysql_data  m
        | left join
        |     $hiveDb.$hiveTableName h
        | on
        |     m.ORDERKEY = h.orderkey
        | where
        |     h.orderkey is null
        | and
        |     date_format(m.ORDERDATE, 'yyyy-MM-dd') >= '1970-01-01'
        |""".stripMargin).createOrReplaceTempView("add_mysql")


    // 开启hive的动态分区
    spark.sql("set hive.exec.dynamic.partition=true")
    // 关闭严格模式
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    spark.sql(s"use $hiveDb")
    spark.sql(
      s"""
        |insert into table $hiveDb.$hiveTableName partition(day)
        |select *, date_format(ORDERKEY, 'yyyyMMdd') from add_mysql
        |""".stripMargin)


    spark.close()




  }
}
