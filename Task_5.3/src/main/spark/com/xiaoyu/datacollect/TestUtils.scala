package com.xiaoyu.datacollect

import org.apache.spark.sql.SparkSession

object TestUtils {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")


    val spark: SparkSession = SparkSession.builder().enableHiveSupport().master("local[*]").appName("插入数据").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("use db")
    spark.sql("select * from emp").show()

//    SparkUtil.saveMysql(emp,"bolin", "emp")




  }
}
