package com.xiaoyu.siheanswer.day13

import java.text.SimpleDateFormat
import java.util.Calendar


import org.apache.spark.sql.{SaveMode, SparkSession}

//spark-submit --master yarn --driver-memory 2G --driver-cores 2  --num-executors 2 --executor-memory 1g --executor-cores 1  --files /opt/module/hive-3.1.2/conf/hive-site.xml  --class com.shtd.day13.day13_offline_calculateJob /opt/dwdatas/teachertrain-1.0-SNAPSHOT.jar yarn
//还需要修改
//hive.exec.max.dynamic.partitions
object day13_offline_calculateJob {
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
    val spark = sparkBuilder.appName("day13_offline_calculateJob")
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
      .option("dbtable", "nationeverymonth").load().createTempView("mysql_nationeverymonth")

    spark.read.format("jdbc")
      .option("url", MYSQLDBURL)
      .option("driver", MYSQLDRIVER)
      .option("user", MYSQLUSER)
      .option("password", MYSQLPASSWORD)
      .option("dbtable", "usercontinueorder").load().createTempView("mysql_usercontinueorder")


    //新版类型转换过于严格，故采用旧版
    spark.sql("set spark.sql.storeAssignmentPolicy=LEGACY")


    /**
     * 一、每个地区、每个国家、每个月下单的数量和下单的总金额
     */
    spark.sql(
      """
        |insert overwrite table mysql_nationeverymonth
        |select nation_key,
        |       max(nation_name) as nation_name,
        |       region_key,
        |       max(region_name) as region_name,
        |       sum(total_price) as totalconsumption,
        |       count(1)         as totalorder,
        |       year,
        |       month
        |from (-- 获取维度信息
        |         select b.nation_key,
        |                c.name                                   as nation_name,
        |                c.region_key,
        |                d.name                                   as region_name,
        |                a.total_price,
        |                date_format(TO_DATE(order_date), 'yyyy') as year,
        |                date_format(TO_DATE(order_date), 'MM')   as month
        |         from dwd.fact_orders a
        |                  left join dwd.dim_customer b
        |                            on a.cust_key = b.cust_key
        |
        |                  left join dwd.dim_nation c
        |                            on b.nation_key = c.nation_key
        |
        |                  left join dwd.dim_region d
        |                            on c.region_key = d.region_key
        |     ) e
        |group by region_key, nation_key, year, month
        |
  """.stripMargin)
    println("一、每个地区、每个国家、每个月下单的数量和下单的总金额")




    /**
     *  二、连续两个  月   下单并且下单金额保持  增长 的用户
     */
    //    System.exit(0)
    //先按照每人每月数据进行聚合，再进行两表相关联,数据较多，先保存在 磁盘上
    spark.sql(
      """
        |select cust_key,
        |       cast(date_format(TO_DATE(order_date), 'yyyyMM') as int) as month,
        |       sum(total_price)                                        as total_price,
        |       count(1)                                                as total_order
        |from dwd.fact_orders
        |where dealdate >= '19980101'
        |group by cust_key, cast(date_format(TO_DATE(order_date), 'yyyyMM') as int)
        |
  """.stripMargin).coalesce(10).write.mode(SaveMode.Overwrite).saveAsTable("dwd.fact_orders_month")
    //    spark.sql("select * from dwd.fact_orders_month limit 10")

    spark.sql(
      """
        |insert overwrite table mysql_usercontinueorder
        |
        |select c.cust_key, d.name, c.month, c.total_price, c.total_order
        |from (
        |         select a.cust_key,
        |                concat(a.month, '_', b.month) as month,
        |                a.total_price + b.total_price as total_price,
        |                a.total_order + b.total_order as total_order
        |         from dwd.fact_orders_month a
        |                  left join dwd.fact_orders_month b  --判断连续两月是否保持增长
        |                            on
        |                                    a.month + 1 = b.month
        |                                    and
        |                                    a.total_price < b.total_price
        |     ) c
        |         left join dwd.dim_customer d on c.cust_key = d.cust_key
  """.stripMargin)


  }
}
