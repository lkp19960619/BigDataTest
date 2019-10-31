package com.baizhi

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HBase创建Spark RDD对象
  */
object RDDDataSourceWithHBase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd with hbase")
    val sc = new SparkContext(conf)

    //1、准备HBase配置对象
    val hbaseConfig = HBaseConfiguration.create()
    //HBase所在的节点
    hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, "HadoopNode00")
    //HBase操作的端口号
    hbaseConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    //操作的表名
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, "baizhi:t_user")
    //操作的列名
    hbaseConfig.set(TableInputFormat.SCAN_COLUMNS, "cf1:name cf1:age cf1:sex")

    //2、通过sc操作hadoop方法构建RDD对象
    val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd.foreach(t => {
      //从HBase中取出来的都是字节，需要转换
      val rowKey = Bytes.toString(t._1.get())
      val name = Bytes.toString(t._2.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("name")))
      val age = Bytes.toString(t._2.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("age")))
      val sex = Bytes.toString(t._2.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("sex")))

      println(s"$rowKey  $name  $age  $sex")
    })
    sc.stop()
  }
}
