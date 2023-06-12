package com.xiaoyu.datacollect

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateTable {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "创建dwd表")

    val odsDb = "ods_temp"
    val dwdDb = "dwd"

    val odsTables: List[String] = List("customer", "part", "nation", "region", "orders", "lineitem")
    val dwdTables: List[String] = List("dim_customer", "dim_part", "dim_nation", "dim_region", "fact_orders", "fact_lineitem")

    val addColumns: List[String] = List("dwd_insert_user", "dwd_insert_time", "dwd_modify_user", "dwd_modify_time")

    val partition = "etl_date"

    for(i <- odsTables.indices) {
      val odsTableName: String = odsTables(i)
      val dwdTableName: String = dwdTables(i)

      val dataFrame: DataFrame = getOdsData(spark, odsDb, odsTableName)
      val tempTableName = s"ods_$odsTableName"
      dataFrame.createOrReplaceTempView(tempTableName)
      val columnList: List[String] = dataFrame.columns.toList

      createDwdTable(spark, dwdDb, dwdTableName, columnList, addColumns, partition)

    }





  }
  private def getOdsData(spark: SparkSession, odsDb: String, odsTableName: String): DataFrame = {
    spark.sql(s"select * from $odsDb.$odsTableName")
  }

  //创建dwd表
  private def createDwdTable(spark: SparkSession, dwdDb: String, dwdTableName: String,
                             columnList: List[String], addColumns: List[String], partition: String): Unit = {

    var column = ""
    columnList.foreach(elem => {
      if (elem != partition) {
        column += s"$elem string,"
      }
    })
    addColumns.foreach(elem => {
      column += s"$elem string,"
    })
    column = column.dropRight(1)

    spark.sql(
      s"""
        |create table $dwdDb.$dwdTableName (
        |   $column
        |)
        |PARTITIONED by ($partition string)
        |""".stripMargin)


  }


}
