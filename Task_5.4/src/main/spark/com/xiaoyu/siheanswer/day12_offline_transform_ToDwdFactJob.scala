package com.xiaoyu.siheanswer

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Calendar

//spark-submit --master yarn-client --driver-memory 4G --driver-cores 4  --num-executors 2 --executor-memory 3g --executor-cores 4 \
// --files /opt/apache-hive-2.3.4-bin/conf/hive-site.xml   --class com.shtd.contest.etl.calcjob /sh/provincecontest-1.0-SNAPSHOT.jar yarn
//还需要修改
//hive.exec.max.dynamic.partitions
object day12_offline_transform_ToDwdFactJob {
  val MYSQLIP = "bigdata1"
  val DATABASE = "shtd_store"
  val MYSQLDBURL = "jdbc:mysql://" + MYSQLIP + ":3306/" + DATABASE + "?characterEncoding=UTF-8"
  val MYSQLDRIVER = "com.mysql.jdbc.Driver"
  val MYSQLUSER = "root"
  val MYSQLPASSWORD = "123456"

  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
      sparkBuilder.master("local[*]")
    }
    val spark = sparkBuilder.appName("day12_offline_transform_ToDwdFactJob")
      .enableHiveSupport()
      .getOrCreate()


    val sdf = new SimpleDateFormat("yyyyMMdd")

    val calendar: Calendar = Calendar.getInstance
    calendar.add(Calendar.DATE, -(1))
    val date = calendar.getTime
    val datestr = sdf.format(date)
    //    对于dwd层

    //新版spark对于数据格式转换有强制要求，可采取旧版的
    spark.sql(" set spark.sql.storeAssignmentPolicy=LEGACY")


    //自定义函数
    val makeDT = (date: String) => {
      date.replaceAll("-", "")
    }
    spark.udf.register("makeDt", makeDT(_: String))

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=50000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=10000")
    spark.sql(
      s"""
         |select
         |orderkey,custkey,orderstatus,totalprice,
         |from_unixtime(unix_timestamp(concat(orderdate," 00:00:00")),'yyyy-MM-dd HH:mm:ss'),
         |orderpriority,clerk,shippriority,comment,
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
         |dealdate
         |from  ods.orders
      """.stripMargin).coalesce(50).createOrReplaceTempView("ods_order_coalesce")

    spark.sql(
      """
        |insert overwrite table dwd.fact_orders partition (dealdate)
        |select * from ods_order_coalesce
        |
      """.stripMargin)


    spark.sql(
      s"""
         |insert overwrite table dwd.fact_lineitem partition (etldate)
         |select
         |orderkey,partkey,suppkey,linenumber,quantity,extendedprice,discount,
         |tax,returnflag,linestatus,
         |from_unixtime(unix_timestamp(concat(shipdate," 00:00:00")),'yyyy-MM-dd HH:mm:ss'),
         |from_unixtime(unix_timestamp(concat(commitdate," 00:00:00")),'yyyy-MM-dd HH:mm:ss'),
         |from_unixtime(unix_timestamp(concat(receiptdate," 00:00:00")),'yyyy-MM-dd HH:mm:ss'),
         |shipinstruct,shipmode,comment,
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
         |etldate
         |from  ods.lineitem
      """.stripMargin)

    //删除分区
    val partitons = spark.sql("show partitions ods.ORDERS").collect.filter(x => {
      null != x && x.length != 0
    }).map(x => {
      println("zheshi____________________________分区：" + x)
      x.toString().replace("]", "")
        .replace("[", "").split("=")(1)
    }).sortBy[Int](x => {
      x.toString.toInt
    })

    if (partitons.length > 3) {
      for (index <- 0 until partitons.length - 3) {
        spark.sql(s"ALTER TABLE ods.orders DROP PARTITION (dealdate='${partitons(index)}')")
      }
    }


    spark.sql(
      s"""
         |insert overwrite table dwd.dim_customer partition (etldate='${datestr}')
         |
         |select
         |custkey,name,address,nationkey,
         |phone,acctbal,mktsegment,comment,
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')
         |from ods.customer
  """.stripMargin)


    spark.sql(
      s"""
         |insert overwrite table dwd.dim_nation partition (etldate='${datestr}')
         |select
         |nationkey,name,regionkey,comment,
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')
         |from  ods.nation
  """.stripMargin)


    spark.sql(
      s"""
         |insert overwrite table dwd.dim_region partition (etldate='${datestr}')
         |select
         |regionkey,name,comment,
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'),
         |'user1',from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')
         |from  ods.region
  """.stripMargin)



  }
}
