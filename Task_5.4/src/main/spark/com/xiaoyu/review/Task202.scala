package com.xiaoyu.review

import com.ibm.icu.text.SimpleDateFormat
import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Date


object Task202 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val odsDb = "ods"
    val dwdDb = "dwd"

    val odsTables: List[String] = List("table1", "table2", "table3")
    val dwdTables: List[String] = List("dim_table1", "dim_table2", "dim_table3")

    val partition = "etl_date"
    val addColumns: List[String] = List("dwd_insert_user", "dwd_insert_time", "dwd_modify_user", "dwd_modify_time")


    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "模块D 任务一： 数据清洗")


    for (i <- odsTables.indices) {
      val odsTableName: String = odsTables(i)
      val dwdTableName: String = dwdTables(i)

      val dataFrame: DataFrame = getOdsData(spark, odsDb, odsTableName)
      val tempTableName = "temp"
      dataFrame.createOrReplaceTempView(tempTableName)
      val columnList: List[String] = dataFrame.columns.toList

      // 创建表
      createDwdTable(spark, partition, columnList, addColumns, dwdDb, dwdTableName)

      // 将数据插入
      writeDataToDwd(spark, partition, columnList, dwdDb, dwdTableName)


    }


  }


  def writeDataToDwd(spark: SparkSession, partition:String, columnList: List[String],
                     dwdDb: String, dwdTableName: String): Unit = {
    var selectColumn = ""

    columnList.foreach(elem => {
      if (elem != partition) {
        if (elem.contains("date")) {
          selectColumn += s"date_format('$elem', 'yyyy-MM-dd HH:mm:ss'), "
        } else {
          selectColumn += s"$elem,"
        }
      }
    })
    selectColumn = selectColumn.dropRight(1)

    val date: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    spark.sql(
      s"""
        |insert into table $dwdDb.$dwdTableName partition($partition)
        |select  $selectColumn, 'user1', '$date', 'user1', '$date', $partition  from temp
        |where $partition = '20230509'
        |""".stripMargin)


  }


  // 根据增加的列和ods原有的列创建出dwd层的表
  def createDwdTable(spark: SparkSession, partition:String, columnList: List[String],
                     addColumns: List[String], dwdDb: String, dwdTableName: String): Unit = {
    var column = ""
    columnList.foreach((elem: String) => {
      if (elem != partition) {
        column += s"$elem string,"
      }
    })

    addColumns.foreach((elem: String) => {
      column += s"$elem string,"
    })

    column = column.dropRight(1)

    spark.sql(s"use $dwdDb")
    spark.sql(
      s"""
        |create table $dwdTableName(
        |    $column
        |)
        |partition by ($partition string)
        |""".stripMargin)
  }


  // 读取ods层的数据
  private def getOdsData(spark: SparkSession, odsDb: String, odsTableName: String): DataFrame = {
    spark.sql(s"select * from $odsDb.$odsTableName")
  }



}
