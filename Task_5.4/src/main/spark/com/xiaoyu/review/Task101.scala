package com.xiaoyu.review

import com.ibm.icu.text.SimpleDateFormat
import com.ibm.icu.util.Calendar
import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object Task101 {
  def main(args: Array[String]): Unit = {

    val mysqlDb = "db"
    val hiveDb = "ods"

    val mysqlTables: List[String] = List("table1", "table2", "table3", "table5", "table6", "table7", "table8", "table9", "table10", "table11")
    val hiveTables: List[String] = List("table1", "table2", "table3", "table5", "table6", "table7", "table8", "table9", "table10", "table11")

    // 分区字段
    val partition = "etl_date"

    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    // 获得分区字段值
    val partitionVal: String = new SimpleDateFormat("yyyyMMdd").format(calendar)

    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "模块B: 任务一 数据采集1-11")

    for (i <- mysqlTables.indices) {

      val mysqlTableName: String = mysqlTables(i)
      val hiveTableName: String = hiveTables(i)

      val tempTableName = s"mysql_$mysqlTableName"
      SparkUtils.readMysql(spark, mysqlDb, mysqlTableName).createOrReplaceTempView(tempTableName)



      // 查询出增量数据
      spark.sql(
        s"""
          |select
          |    m.*
          |from
          |    $tempTableName m
          |left join
          |    $hiveDb.$hiveTableName h
          |on
          |    m.modified_time = h.modified_time
          |where
          |    h.modified_time is null
          |""".stripMargin).createOrReplaceTempView(tempTableName)


      // 将数据写入到hive的ods层
      spark.sql(
        s"""
          |insert into table $hiveDb.$hiveTableName partition($partition=$partitionVal)
          |select * from $tempTableName
          |""".stripMargin)

    }



  }
}
