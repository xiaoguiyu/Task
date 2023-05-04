package com.xiaoyu.test

import com.xiaoyu.utils.SparkUtil
import org.apache.spark.sql.SparkSession

/**
 * @version 1.0
 * @author xiaoyu
 * 数据清洗
 */
object TestTask {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().appName("任务一：全量抽取(1-6)").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 需要操作的mysql库名和hive的库名
    val mysqlDb: String = "bolin"
    val hiveDb: String = "ods"

    // mysql的表
    val mysqlTables: List[String] = List("dept_test", "emp")
    // hive 的表
    val hiveTables: List[String] = List("dept", "emp")


    for (i <- mysqlTables.indices) {
      val mysqlTable: String = mysqlTables(i)
      val tempTable = s"mysql_$mysqlTable"
      // 将从mysql中查出的数据保存到spark的临时表中
      SparkUtil.readMysql(spark, mysqlDb, mysqlTables(i)).createOrReplaceTempView(tempTable)

      // 将数据保存到hive中（全量抽取）
      SparkUtil.saveHive(spark, hiveDb, hiveTables(i), tempTable, "day", "20230509")
    }

    spark.close()
  }

}