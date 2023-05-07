package com.xiaoyu.siheanswer

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Calendar

//spark-submit --master yarn-client --driver-memory 4G --driver-cores 4 --num-executors 2 --executor-memory 3g --executor-cores 4 --files /opt/apache-hive-2.3.4-bin/conf/hive-site.xml  --class com.shtd.contest.etl.extractjob /sh/provincecontest-1.0-SNAPSHOT.jar yarn
//还需要修改
//hive.exec.max.dynamic.partitions
object day12_offline_extract_ToOdsOrderCorrelationJob {

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
    val spark = sparkBuilder.appName("day12_offline_extract_ToOdsOrderCorrelationJob")
      .enableHiveSupport()
      .getOrCreate()


    /**
     * 连接mysql
     * */
    spark.read.format("jdbc")
      .option("url", MYSQLDBURL)
      .option("driver", MYSQLDRIVER)
      .option("user", MYSQLUSER)
      .option("password", MYSQLPASSWORD)
      .option("dbtable", "ORDERS").load().createTempView("mysql_orders")

    spark.read.format("jdbc")
      .option("url", MYSQLDBURL)
      .option("driver", MYSQLDRIVER)
      .option("user", MYSQLUSER)
      .option("password", MYSQLPASSWORD)
      .option("dbtable", "LINEITEM").load().createTempView("mysql_lineitem")


    spark.read.format("jdbc")
      .option("url", MYSQLDBURL)
      .option("driver", MYSQLDRIVER)
      .option("user", MYSQLUSER)
      .option("password", MYSQLPASSWORD)
      .option("dbtable", "CUSTOMER").load().createTempView("mysql_customer")

    spark.read.format("jdbc")
      .option("url", MYSQLDBURL)
      .option("driver", MYSQLDRIVER)
      .option("user", MYSQLUSER)
      .option("password", MYSQLPASSWORD)
      .option("dbtable", "NATION").load().createTempView("mysql_nation")



    spark.read.format("jdbc")
      .option("url", MYSQLDBURL)
      .option("driver", MYSQLDRIVER)
      .option("user", MYSQLUSER)
      .option("password", MYSQLPASSWORD)
      .option("dbtable", "REGION").load().createTempView("mysql_region")


    val sdf = new SimpleDateFormat("yyyyMMdd")

    val calendar: Calendar = Calendar.getInstance
    calendar.add(Calendar.DATE, -(1))
    val date = calendar.getTime
    val datestr = sdf.format(date)


    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=50000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=10000")
    spark.sql(
      s"""
         |select
         |orderkey,custkey,orderstatus,totalprice,orderdate,
         |orderpriority,clerk,shippriority,comment,regexp_replace(orderdate,"-","")
         | from mysql_orders a
         |left join
         |(select COALESCE(max(orderkey),-1) as maxid from  ods.orders ) b
         |on 1=1 where cast(a.orderkey as int)>cast(b.maxid as int)
         |and regexp_replace(a.orderdate,"-","")>= '19971201'
         |
      """.stripMargin).coalesce(50).createOrReplaceTempView("mysql_order_coalesce")


    spark.sql(
      """
        |insert overwrite table ods.orders partition (dealdate)
        |select * from mysql_order_coalesce
        |
      """.stripMargin)


    spark.sql(
      s"""
         |insert into  table ods.lineitem partition (etldate='${datestr}')
         |select a.* from mysql_lineitem a
         |left join
         |(select COALESCE(max(orderkey),-1) as maxid from ods.lineitem) b
         |on 1=1 where cast(a.orderkey as int)>cast(b.maxid as int)
         |
      """.stripMargin)




    spark.sql(
      s"""
         |insert overwrite  table ods.customer partition (etldate='${datestr}')
         |select * from mysql_customer
         |
      """.stripMargin)

    spark.sql(
      s"""
         |insert overwrite  table ods.nation partition (etldate='${datestr}')
         |select * from mysql_nation
         |
      """.stripMargin)



    spark.sql(
      s"""
         |insert overwrite  table ods.region partition (etldate='${datestr}')
         |select * from mysql_region
         |
      """.stripMargin)

  }


}
