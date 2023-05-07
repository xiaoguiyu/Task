package com.xiaoyu.test

import com.xiaoyu.utils.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object TestUtils {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")

    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "删除分区")

    val rdd: Array[Row] = spark.sql("show partitions test.dept").rdd.collect()
    val filterRow: Array[Row] = rdd.filter(elem => {
      elem != null && elem.length > 0
    })

    println(filterRow.mkString(", "))

    val strings: Array[String] = filterRow.map(elem => {
      elem.toString().replace("[", "").replace("]", "")
    })






  }
}
