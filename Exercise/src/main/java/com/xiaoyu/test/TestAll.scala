package com.xiaoyu.test

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestAll {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val mysqlDb = "db"
    val odsDb = "ods_temp"
    val partition = "elt_date"

    val mysqlTables: List[String] = List("CUSTOMER", "NATION", "PART", "PARTSUPP", "REGION", "SUPPLIER", "ORDERS", "LINEITEM")
    val odsTables: List[String] = List("customer", "nation", "part", "partsupp", "region", "supplier", "orders", "lineitem")


    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "创建表")

    spark.sql("show databases").show()


    for (i <- mysqlTables.indices) {
      val mysqlTableName: String = mysqlTables(i)
      val odsTableName: String = odsTables(i)

      val dataFrame: DataFrame = SparkUtils.readMysql(spark, mysqlDb, mysqlTableName)
      val columnList: List[String] = dataFrame.columns.toList
      createDwdTable(spark, partition, columnList, odsDb, odsTableName)
    }
  }


  private def createDwdTable(spark: SparkSession, partition: String, columnList: List[String],
                     dwdDb: String, dwdTableName: String): Unit = {

    var column = ""
    columnList.foreach((elem: String) => {
      if (elem != partition) {
        column += s"$elem string,"
      }
    })
    column = column.dropRight(1)
    spark.sql(s"use $dwdDb")
    spark.sql(
      s"""
         |create table $dwdTableName(
         |    $column
         |)
         |partitioned by ($partition string)
         |""".stripMargin)
  }




}
