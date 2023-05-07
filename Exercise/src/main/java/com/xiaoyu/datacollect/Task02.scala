package com.xiaoyu.datacollect

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object Task02 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val mysqlDb = "db"
    val odsDb = "ods_temp"

    val mysqlTables: List[String] = List("ORDERS", "LINEITEM")
    val odsTables: List[String] = List("orders", "lineitem")

    val partition = "elt_date"

    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "任务一： 数据采集7-8")
    for (i <- mysqlTables.indices) {
      val mysqlTableName: String = mysqlTables(i)
      val odsTableName: String = odsTables(i)

      val tempTableName = s"mysql_$mysqlTableName"
      SparkUtils.readMysql(spark, mysqlDb, mysqlTableName).createOrReplaceTempView(tempTableName)

      spark.sql("set hive.exec.dynamic.partition=true")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstric")

      val list: List[String] = spark.sql(s"select * from $tempTableName").columns.toList
      println(list.mkString(","))


       if (mysqlTableName == "ORDERDATE") {
         spark.sql(
           s"""
              |select
              |    m.*
              |from
              |    $tempTableName m
              |left join
              |    (select max(orderkey) max_key from $odsDb.$odsTableName) h
              |on
              |    1 = 1
              |where
              |    cast(m.ORDERKEY as int) > cast(h.max_key as int)
              |and
              |    date_format(m.ORDERDATE, 'yyyyMMdd') >= '19700504'
              |""".stripMargin).createOrReplaceTempView("add_temp")


         spark.sql(
           s"""
              |insert into table $odsDb.$odsTableName partition($partition)
              |select *, date_format(ORDERDATE, 'yyyyMMdd') from add_temp
              |""".stripMargin)


       } else  {
         spark.sql(
           s"""
              |select
              |    m.*
              |from
              |    $tempTableName m
              |left join
              |    (select max(orderkey) max_key from $odsDb.$odsTableName) h
              |on
              |    1 = 1
              |where
              |    cast(m.ORDERKEY as int) > cast(h.max_key as int)
              |""".stripMargin).createOrReplaceTempView("add_temp")

         spark.sql(
           s"""
              |insert into table $odsDb.$odsTableName partition($partition='20230509')
              |select * from add_temp
              |""".stripMargin)

       }




   }









    spark.close()

  }




}
