//package com.xiaoyu.siheanswer
//
//import com.shtd.ConfUtils
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.CheckpointingMode
//import org.apache.flink.streaming.api.functions.ProcessFunction
//import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.util.Collector
//
//import java.io.File
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//import scala.util.Random
//
//
////kafka-topics.sh --create --zookeeper bigdata1:2181/kafka --replication-factor 1 --partitions 2 --topic ods_ordersall_kafka
////kafka-topics.sh --create --zookeeper bigdata1:2181/kafka --replication-factor 1 --partitions 2 --topic fact_orders
////kafka-console-consumer.sh  --bootstrap-server bigdata1:9092 --group test2 --topic ods_ordersall_kafka
////nohup flume-ng agent --name agent -c conf -f /opt/module/flume-1.9.0/conf/flume-talent-kafka.conf >/opt/module/flume-1.9.0/logs/flume-kafka.log &
////nohup ./to_make_socket_data_v2 >/opt/module/flume-1.9.0/logs/socket.log &
////flink run -m yarn-cluster -p 2 -yjm 2G -ytm 2G -yn -c com.shtd.dw.online.dwd.ToDWdAndHbaseStream /sh/provincecontest-1.0-SNAPSHOT.jar yarn
//
//case class Orders(row_key: String,
//                  orderkey: String,
//                  custkey: String,
//                  orderstatus: String,
//                  totalprice: String,
//                  orderdate: String,
//                  orderpriority: String,
//                  clerk: String,
//                  shippriority: String,
//                  comment: String,
//                  create_user: String,
//                  create_time: String,
//                  modify_user: String,
//                  modify_time: String,
//                  ods_insert_time: String,
//                  ods_update_time: String
//                 )
//
//case class LineItem(row_key: String,
//                    orderkey: String,
//                    partkey: String,
//                    suppkey: String,
//                    linenumber: String,
//                    quantity: String,
//                    extendedprice: String,
//                    discount: String,
//                    tax: String,
//                    returnflag: String,
//                    linestatus: String,
//                    shipdate: String,
//                    commitdate: String,
//                    receiptdate: String,
//                    shipinstruct: String,
//                    shipmode: String,
//                    comment: String,
//                    create_user: String,
//                    create_time: String,
//                    modify_user: String,
//                    modify_time: String,
//                    ods_insert_time: String,
//                    ods_update_time: String
//                   )
//
//object day12_ToDWdAndHbaseStream {
//
//
//  def main(args: Array[String]): Unit = {
//    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    var tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
//
//    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
//      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
//      tableEnv = StreamTableEnvironment.create(env)
//    }
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", ConfUtils.BOOTSTRAPSERVERS)
//    properties.setProperty("group.id", ConfUtils.GROUPID)
//
//    //    env.setParallelism(6)
//    env.enableCheckpointing(5000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    val kafkaStream = env
//      .addSource(new FlinkKafkaConsumer[String](ConfUtils.ODSTOPIC, new SimpleStringSchema(), properties))
//
//
//    tableEnv.executeSql(
//      s"""
//         |CREATE TABLE kafkaOrderTable (
//         |order_key BIGINT,
//         |cust_key BIGINT,
//         |order_status String,
//         |total_price DOUBLE,
//         |order_date TIMESTAMP(14),
//         |order_priority String,
//         |clerk String,
//         |ship_priority String,
//         |`comment` String,
//         |create_user String,
//         |create_time TIMESTAMP(14),
//         |modify_user String,
//         |modify_time TIMESTAMP(14)
//         |) WITH (
//         | 'connector' = 'kafka',
//         | 'topic' = '${ConfUtils.DWDTOPIC}',
//         | 'properties.bootstrap.servers' = '${ConfUtils.BOOTSTRAPSERVERS}',
//         | 'properties.group.id' = '${ConfUtils.GROUPID}',
//         | 'format' = 'json',
//         | 'scan.startup.mode' = 'earliest-offset'
//         |)
//         |
//         |
//      """.stripMargin)
//
//    tableEnv.executeSql(
//      s"""
//         | CREATE TABLE hbaseOrderTable (
//         | rowkey  String,
//         | info ROW<orderkey String,
//         | custkey String,
//         | orderstatus String,
//         | totalprice String,
//         | orderdate String,
//         | orderpriority String,
//         | clerk String,
//         | shippriority String,
//         | `comment` String,
//         | create_user String,
//         | create_time String,
//         | modify_user String,
//         | modify_time String,
//         | ods_insert_time String,
//         | ods_update_time String>
//         |) WITH (
//         |'connector'='hbase-2.2',
//         |'table-name'='ods:orders_hbase',
//         |'zookeeper.quorum'='bigdata1:2181',
//         |'zookeeper.znode.parent'='/hbase'
//         |)
//          """.stripMargin)
//
//
//    tableEnv.executeSql(
//      s"""
//         | CREATE TABLE hbaseLineitemTable (
//         | row_key String,
//         | info ROW<orderkey String, partkey String, suppkey String,
//         | linenumber String,quantity String,extendedprice String,
//         | discount String,tax String,returnflag String,linestatus String,
//         | shipdate  String,commitdate  String,receiptdate  String,
//         | shipinstruct  String,shipmode  String,
//         | `comment` String,create_user  String,create_time  String,
//         | modify_user  String,modify_time  String,
//         | ods_insert_time String,ods_update_time  String>
//         |) WITH (
//         |  'connector' = 'hbase-2.2',
//         | 'table-name' = 'ods:lineitem_hbase',
//         | 'zookeeper.quorum' = 'bigdata1:2181',
//         | 'zookeeper.znode.parent' = '/hbase'
//         |)
//          """.stripMargin)
//
//
//    //泛型为侧输出流要输出的数据格式
//    val tag: OutputTag[LineItem] = new OutputTag[LineItem]("lineitem")
//    val mainStream: DataStream[Orders] = kafkaStream.process(new getSide(tag))
//    val sideStream: DataStream[LineItem] = mainStream.getSideOutput(tag)
//    tableEnv.createTemporaryView("Orders", mainStream)
//    tableEnv.createTemporaryView("LineItem", sideStream)
//    val stat = tableEnv.createStatementSet()
//
//    stat.addInsertSql(
//      """
//        |insert INTO kafkaOrderTable
//        |
//        |select
//        | cast(orderkey as bigint) orderkey ,
//        | cast(custkey as bigint) custkey,
//        | orderstatus ,
//        | cast(totalprice as double) totalprice,
//        | cast(concat(orderdate,' 00:00:00.221111') as Timestamp(14)) orderdate ,
//        | orderpriority ,
//        | clerk ,
//        | shippriority ,
//        | `comment` ,
//        | create_user ,
//        | cast(create_time as Timestamp)  create_time,
//        | modify_user ,
//        | cast(modify_time  as Timestamp) modify_time
//        |
//        | from Orders
//      """.stripMargin)
//    stat.addInsertSql(
//      """
//        |insert INTO hbaseOrderTable
//        |select
//        |row_key,
//        |ROW(orderkey,
//        |custkey,
//        | orderstatus,
//        |totalprice,
//        |orderdate,
//        | orderpriority,
//        | clerk,
//        |shippriority,
//        |`comment`,
//        |create_user,
//        |create_time,
//        | modify_user,
//        | modify_time,
//        |ods_insert_time,
//        |ods_update_time)
//        |from Orders
//      """.stripMargin)
//
//    stat.addInsertSql(
//      """
//        |insert INTO hbaseLineitemTable
//        |select
//        |row_key,
//        |ROW(orderkey , partkey , suppkey ,
//        | linenumber ,quantity ,extendedprice ,
//        | discount ,tax ,returnflag ,linestatus ,
//        | shipdate  ,commitdate  ,receiptdate  ,
//        | shipinstruct  ,shipmode  ,
//        | `comment` ,create_user  ,create_time  ,
//        | modify_user  ,modify_time  ,
//        | ods_insert_time ,ods_update_time  )
//        |from LineItem
//      """.stripMargin)
//    stat.execute()
//
//
//    env.execute("ToDwdAndHbaseStream")
//  }
//
//
//}
//
////获取侧边流
//class getSide(SideStream: OutputTag[LineItem]) extends ProcessFunction[String, Orders] {
//  override def processElement(value: String, ctx: ProcessFunction[String, Orders]#Context, out: Collector[Orders]): Unit = {
//    //其中rowkey为随机数+订单日期+当前时分秒 来确保不会发生重复，且不会发生数据聚集在一个节点上
//    val rowkeypre = Random.nextInt(10)
//    val sdf = new SimpleDateFormat("yyyyMMdd")
//    val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
//    val sdf3 = new SimpleDateFormat("HHmmssSSS")
//
//    val time = sdf2.format(new Date())
//    val midtime = sdf3.format(new Date())
//    if ("O".equals(value.split(":")(0))) {
//      //这是主流
//      val vrep = value.replaceFirst("O:", "")
//      val v=vrep.split(",")
//      //      val v = value.replaceFirst("O:", "").split(",")
//      out.collect(Orders(rowkeypre +v(4).replaceAll("'", "").replaceAll("-","") + midtime,
//        v(0).replace("'", ""), v(1).replace("'", ""),
//        v(2).replace("'", ""), v(3).replace("'", ""), v(4).replace("'", ""),
//        v(5).replace("'", ""), v(6).replace("'", ""), v(7).replace("'", ""),
//        vrep.slice(vrep.indexOf(v(8)),vrep.length),
//        "user", time, "user", time, time, time)
//
//      )
//
//    } else if ("L".equals(value.split(":")(0))) {
//      //这是边流
//      val vrep = value.replaceFirst("L:", "")
//      val v=vrep.split(",")
//      //      val v = value.replaceFirst("L:", "").split(",")
//      ctx.output(SideStream,
//        LineItem(rowkeypre + v(10).replaceAll("'", "").replaceAll("-","") + midtime,
//          v(0).replace("'", ""),
//          v(1).replace("'", ""),
//          v(2).replace("'", ""), v(3).replace("'", ""), v(4).replace("'", ""),
//          v(5).replace("'", ""), v(6).replace("'", ""), v(7).replace("'", ""),
//          v(8).replace("'", ""), v(9).replace("'", ""), v(10).replace("'", ""),
//          v(11).replace("'", ""), v(12).replace("'", ""),v(13).replace("'", ""),
//          v(14).replace("'", ""),
//          vrep.slice(vrep.indexOf(v(15)),vrep.length)
//          , "user",time, "user",time, time, time))
//
//
//    }
//
//
//  }
//}
//
//
