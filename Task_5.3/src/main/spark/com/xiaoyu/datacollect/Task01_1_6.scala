package com.xiaoyu.datacollect

import com.xiaoyu.utils.SparkUtil
import org.apache.spark.sql.SparkSession

/**
 * @version 1.0
 * @author xiaoyu
 * 数据清洗
 */
object Task01_1_6 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().master("local[*]").appName("任务一：全量抽取").getOrCreate()

    // 需要操作的mysql库名和hive的库名
    val mysqlDb: String = "shtd_store"
    val hiveDb: String = "ods"

    // mysql的表
    val mysqlTables: List[String] = List("CUSTOMER", "NATION","PART","PARTSUPP","REGION","SUPPLIER")
    // hive 的表
    val hiveTables: List[String] = List("customer", "nation","part", "partsupp", "region", "supplier")


    for (i <- 0 to mysqlTables.length ) {
      val tempTable = s"mysql_$mysqlTables"
      // 将从mysql中查出的数据保存到spark的临时表中
      SparkUtil.readMysql(spark, mysqlDb, mysqlTables(i)).createOrReplaceTempView(tempTable)

      // 将数据保存到hive中（全量抽取）
      SparkUtil.saveHive(spark, hiveDb, hiveTables(i), tempTable, "day", "20230503")
    }



    spark.close()
  }




}
