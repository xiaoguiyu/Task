package com.xiaoyu.datacollect

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "数据清洗")

    val odsDb = "ods_temp"
    val dwdDb = "dwd"

    val odsTables: List[String] = List("customer", "part", "nation", "region", "orders")
    val dwdTables: List[String] = List("dim_customer", "dim_part", "dim_nation", "dim_region", "fact_orders")

    val partition = "etl_date"
    val partitionVal: String = SparkUtils.getyyyyMMddDate



    for (i <- odsTables.indices) {
      val odsTableName:String = odsTables(i)
      val dwdTableName:String = dwdTables(i)

      // 临时表名
      val tempTableName = s"ods_$odsTableName"

      val dataFrame: DataFrame = getOdsData(spark, odsDb, odsTableName)
      val columnList: List[String] = dataFrame.columns.toList
      dataFrame.createOrReplaceTempView(tempTableName)


      writeDwd(spark, tempTableName, dwdDb, dwdTableName, partition, partitionVal, columnList)
    }

  }



  private def getOdsData(spark:SparkSession, odsDb: String, odsTableName: String): DataFrame = {
    spark.sql(s"select * from $odsDb.$odsTableName")
  }



  private def writeDwd(spark:SparkSession,  tempTableName: String, dwdDb: String, dwdTableName: String,
                       partition: String, partitionVal: String, columnList: List[String]): Unit = {

    var column = ""
    columnList.foreach(elem => {
      if (elem != partition) {
        if (elem.contains("date")) {
          column += s"from_unixtime(unix_timestamp($elem, 'yyyy-MM-dd')),"
        } else {
          column += s"$elem,"
        }
      }
    })
    column = column.dropRight(1)


    val date: String = SparkUtils.getHHmmssDate
    if (dwdTableName == "orders") {
      spark.sql(
        s"""
           |insert into table $dwdDb.$dwdTableName partition($partition)
           |select $column, 'user1', '$date', 'user1', '$date', $partition  from $tempTableName
           |""".stripMargin)
    } else  {

      spark.sql(
        s"""
           |insert into table $dwdDb.$dwdTableName partition($partition=$partitionVal)
           |select $column, 'user1', '$date', 'user1', '$date' from $tempTableName
           |""".stripMargin)
    }




  }





}
