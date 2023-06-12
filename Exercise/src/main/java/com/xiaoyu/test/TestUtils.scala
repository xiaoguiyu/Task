package com.xiaoyu.test

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.sql.SparkSession

object TestUtils {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = true, "spark -- Test")

    spark.sql("select * from test.dept").show()
    spark.sql("show partitions test.dept").show()
    SparkUtils.readMysql(spark, "db", "customer").show(20)



  }
}
