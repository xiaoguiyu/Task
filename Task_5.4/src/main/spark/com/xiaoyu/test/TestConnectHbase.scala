package com.xiaoyu.test

import com.xiaoyu.utils.SparkUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestConnectHbase {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkUtils.getHiveSparkSession(isCluster = false )
    import spark.implicits._

    val hbaseConf: Configuration = HBaseConfiguration.create()

    val hbaseTableName = "stu"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)


    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    // RDD的类型转换
    val tupRDD: RDD[(String, String, String)] = hbaseRDD.map({
      case (_, result) => {
        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")))
        val age: String = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("age")))
        val sex: String = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("sex")))

        // 转换成RDD[Row]
        (name,age, sex)
      }
    })

    val dataFrame: DataFrame = tupRDD.toDF("name", "age", "sex")
    dataFrame.createOrReplaceTempView("temp")

    spark.sql("select * from temp").show()







  }
}
