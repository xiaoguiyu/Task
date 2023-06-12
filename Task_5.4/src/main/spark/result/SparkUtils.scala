package result

import com.ibm.icu.text.SimpleDateFormat
import com.ibm.icu.util.Calendar
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.{Date, Properties}

object SparkUtils {

  /**
   * 获取比赛前一天的日期 格式为yyyyMMdd
   */
  def getyyyyMMddDate: String = {
      val calendar: Calendar = Calendar.getInstance()
      calendar.add(Calendar.DATE, -1)
      new SimpleDateFormat("yyyyMMdd").format(calendar)
  }


  /**
   * 获取当前的日期 格式为 yyyy-MM-dd HH:mm:ss
   */
  def getHHmmssDate: String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
  }

  var conf: SparkConf = _

  /**
   * 获取支持hive的sparkSession
   * @param isCluster 是否以集群模式运行job
   * @return
   */
  def getHiveSparkSession(isCluster: Boolean, appName: String = "task"): SparkSession = {
    if (isCluster) {
      conf = new SparkConf().setAppName(appName)
    } else {
      conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    }
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nostric")
    spark
  }


  private def getProp: Properties = {
    val prop = new Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "123456")
    prop
  }

  /**
   * 读取mysql数据库中相应的表的数据
   * @return
   */
  def readMysql(spark: SparkSession, mysqlDb: String, mysqlTableName: String): DataFrame = {
    spark.read.jdbc(s"jdbc:mysql://172.29.44.24:3306/$mysqlDb?useSSL=false", mysqlTableName, getProp)
  }

  /**
   * 将数据写入到mysql中
   */
  def writeMysql(dataFrame: DataFrame, mysqlDb: String, mysqlTableName: String): Unit = {
    dataFrame.write.mode("overwrite")
      .jdbc(s"jdbc:mysql://172.29.44.24:3306/$mysqlDb?useSSL=false&createDatabaseIfNotExist=true", mysqlTableName, getProp)
  }

  private def getClickHouseProp: Properties = {
    val prop = new Properties()
    prop.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    prop.put("user", "default")
    prop.put("password", "root")
    prop
  }



  def readClickHouse(spark: SparkSession, clickhouseDb: String, clickhouseTableName: String): DataFrame = {
    spark.read.jdbc(s"jdbc:clickhouse://172.29.44.86:8123/$clickhouseDb?useSSL=false", clickhouseTableName, getClickHouseProp)
  }


}
