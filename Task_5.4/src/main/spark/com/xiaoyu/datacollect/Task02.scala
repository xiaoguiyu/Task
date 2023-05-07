package com.xiaoyu.datacollect

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 模块D 任务一： 数据清洗
 */
object Task02 {
  def main(args: Array[String]): Unit = {

    // hive 中ods层库名和表
    val odsDb: String = "ods"
    val odsTables: List[String] = List("table1", "table2", "table3")

    // hive 中dwd层库名和表
    val dwdDb: String = "dwd"
    val dwdTables: List[String] = List("dim_table1", "dim_table2", "dim_table3")

    // 分区字段
    val partition: String = "etl_date"

    // dwd 层表中需要添加的列
    val addColumns: List[String] = List("dwd_insert_user", "dwd_insert_time", "dwd_modify_user", "dwd_modify_time")


    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "模块D 任务一： 数据清洗")

    for (i <- odsTables.indices) {
      val odsTable: String = odsTables(i)
      val dwdTable: String = dwdTables(i)

      // 获取ods层表的数据
      val dataFrame: DataFrame = getOdsTableData(spark, odsDb, odsTable)


      // 获取表列名
        val columnList: List[String] = dataFrame.columns.toList

      // 创建出dwd层的表
      createDwdTable(spark, columnList, addColumns, partition, dwdDb, dwdTable)

      // 进行数据清洗
      writeDataToDwd(spark, dwdDb, dwdTable, columnList, partition, odsDb, odsTable)
    }




  }

  /**
   * * 将数据清洗病并写入到dwd层
   * @param spark      sparkSession
   * @param dwdDb      dwd库名
   * @param dwdTable   dwd表名
   * @param columnList 需要查询的列
   * @param partition  分区字段
   * @param odsDb ods层库名
   * @param odsTable ods层的表
   */
  private def writeDataToDwd(spark: SparkSession, dwdDb: String, dwdTable: String, columnList: List[String],
                             partition: String, odsDb: String, odsTable: String): Unit = {

    var column = ""
    // 将表中含有timestamp类型的日期 格式化
    columnList.foreach(elem => {
      if (elem != partition) {
        if (elem.contains("date")) {
          column = s"date_format('$elem', 'yyyy-MM-dd HH:mm:ss')"
        } else {
          column = s"$elem,"
        }
      }
    })
    column = column.dropRight(1)

    val date: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)

    spark.sql(
      s"""
        | insert into table $dwdDb.$dwdTable partition($partition)
        | select $column, 'user1', '$date', 'user1', '$date', '$partition' from $odsDb.$odsTable
        | where $partition = '20230509'
        | """.stripMargin)


  }

  /**
   * 查询到ods层表的数据
   * @param spark sparkSEssion
   * @param odsDb ods库名
   * @param odsTable ods表名
   * @return 查询出的数据
   */
  private def getOdsTableData(spark: SparkSession, odsDb: String, odsTable: String): DataFrame = {
    spark.sql(s"select * from $odsDb.$odsTable")
  }


  /**
   * 根据ods原有的字段创建出dwd的表
   * @param spark sparkSession
   * @param columns 原有的列
   * @param addColumns 需要添加的列
   * @param partition 分区字段
   * @param dwdDb 库名
   * @param dwdTableName 创建的表名
   */
  private def createDwdTable(spark: SparkSession, columns: List[String], addColumns: List[String],  partition: String, dwdDb: String,  dwdTableName: String): Unit = {

    // 创建表 所需的字段 和类型
    var column = ""

    columns.foreach(elem => {
      if (elem != partition) {
        column += s"$elem string,"
      }
    })

    addColumns.foreach(elem => {
      column += s"$elem string,"
    })

    // 删除最后多余的 ,
    column = column.dropRight(1)

    spark.sql(
      s"""
        |create table $dwdDb.$dwdTableName(
        | $column
        |)
        |PARTITIONED BY ($partition string)
        |""".stripMargin)
  }




}
