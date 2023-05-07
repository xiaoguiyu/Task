package com.xiaoyu.datacollect

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object Task01 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val mysqlDb: String = "db"
    val hiveDb: String = "ods"
    // 注意： 仔细审题！！！ 模块B中的任务一4小题与其他的要求不同， 不能使用此通用方法！
    val mysqlTables: List[String] = List("table1", "table2", "table3",  "table5", "table6", "table7", "table8", "table9", "table10", "table11")
    val hiveTables: List[String] = List("table1", "table2", "table3", "table5", "table6", "table7", "table8", "table9", "table10", "table11")

    // 分区字段
    val partitionFiled: String = "etl_date"
    // 分区字段的值
    val partitionVal: String = "20230509"

    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "任务一： 离线数据采集1-11 -- 静态增量处理")

    for (i <- mysqlTables.indices) {
      val mysqlTable: String = mysqlTables(i)
      val hiveTable: String = hiveTables(i)

      // 临时表名
      val tempTable: String = s"mysql_$mysqlTable"
      // 查询mysql中的数据 并将数据保存到spark的临时表中
      SparkUtils.readMysql(spark, mysqlDb, mysqlTable).createOrReplaceTempView(tempTable)

      // 查询出增量数据 并保存到临时表中
      spark.sql(
        s"""
          | select
          |    m.*
          | from
          |    $tempTable m
          | left join
          |    $hiveDb.$hiveTable h
          | on
          |    m.modified_time = h.modified_time
          | where
          |    h.modified_time is null
          |""".stripMargin).createOrReplaceTempView("add_mysql")


      // 将增量数据写入到hive中
        spark.sql(
          s"""
            | insert into table $hiveDb.$hiveTable partition($partitionFiled='$partitionVal')
            | select * from add_mysql
            |""".stripMargin)
    }

    task01_04(spark)

  spark.close()

  }


  /**
   * 模块B 任务一的第四问
   * @param spark sparkSession
   */
  private def task01_04(spark: SparkSession): Unit = {

    val mysqlDb: String = "db"
    val hiveDb: String = "ods"
    val mysqlTable: String = "table4"
    val hiveTable: String = "table4"
    val partitionFiled: String = "etl_date"
    val partitionVal: String = "20230509"

    // 查询mysql中的数据 并将数据保存到spark的临时表中
    SparkUtils.readMysql(spark, mysqlDb, mysqlTable).createOrReplaceTempView("tempTable")


    // 计算出增量字段
    spark.sql(s"select max(greatest(time1, time2)) as max_time from $hiveTable").createOrReplaceTempView("add_filed")

    // 查询出增量数据 并保存到临时表中

    // todo 询问！存在问题？
    spark.sql(
      s"""
         | select
         |    *
         | from
         |    tempTable
         | where
         |    time1 > (select max_time from add_filed)
         | or
         |    time2 > (select max_time from add_filed)
         |""".stripMargin).createOrReplaceTempView("add_mysql")

    // 将增量数据保存到hive中
    spark.sql(
      s"""
        | insert into table $hiveDb.$hiveTable partition($partitionFiled='$partitionVal')
        | select * from add_mysql
        |""".stripMargin)

  }


}