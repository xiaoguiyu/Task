package com.xiaoyu.datacollect

import com.xiaoyu.utils.SparkUtil
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark: SparkSession = SparkUtil.getHiveSparkSession(isCluster = false)
    spark.sql("show databases").show()


    SparkUtil.readMysql(spark, "spark_sql", "dept").show()


  }
}
