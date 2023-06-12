package test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import result.SparkUtils

object TestAll {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false, "test")
    val tables: List[String] = List("coupon_info", "coupon_use", "customer_addr", "customer_login_log", "dim_customer_addr",
      "dim_customer_inf", "dim_customer_level_inf", "dim_product_info", "fact_coupon_use", "fact_order_cart", "fact_order_detail",
      "fact_order_master", "log_customer_login", "log_product_browse", "order_cart" , "order_detail", "order_master", "product_browse", "product_info" )

    for (elem <- tables) {
      spark.sql(s"drop table ods.$elem")
    }


    val context: SparkContext = spark.sparkContext

    val rdd: RDD[Row] = spark.sql(s"drop table ods.$tables").rdd





  }
}
