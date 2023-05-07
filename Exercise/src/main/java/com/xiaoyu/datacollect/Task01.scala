package com.xiaoyu.datacollect

import com.ibm.icu.text.SimpleDateFormat
import com.ibm.icu.util.Calendar
import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object Task01 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val mysqlDb = "db"
    val odsDb = "ods_temp"

    val mysqlTables: List[String] = List("CUSTOMER", "NATION", "PART", "PARTSUPP", "REGION", "SUPPLIER")
    val odsTables: List[String] = List("customer", "nation", "part", "partsupp", "region", "supplier")

    val partition = "elt_date"
    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)

    val partitionVal: String = new SimpleDateFormat("yyyyMMdd").format(calendar)


    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "任务一： 数据采集1-6")

    for (i <- mysqlTables.indices) {
      val mysqlTableName: String = mysqlTables(i)
      val odsTableName: String = odsTables(i)

      val tempTableName = s"mysql_$mysqlTableName"
      SparkUtils.readMysql(spark, mysqlDb, mysqlTableName).createOrReplaceTempView(tempTableName)

      spark.sql(
        s"""
          |insert overwrite table $odsDb.$odsTableName partition($partition=$partitionVal)
          |select * from $tempTableName
          |""".stripMargin)



    }






    spark.close()

  }




}
