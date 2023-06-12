package com.xiaoyu.test

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.SparkSession


object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")


    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "spark -- Test")

    spark.sql("select * from ods.region").show()
    spark.sql("show partitions test.dept").show()







  }
}
