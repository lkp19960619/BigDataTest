package com.baizhi.outputdatasource

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisPool

/**
  * 计算结果输出测试
  */
object OutputTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("output test application").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    //设置日志等级
    ssc.sparkContext.setLogLevel("ERROR")

    val dStream = ssc.socketTextStream("Spark01", 5555)
    //HDFS集群配置信息
    val configuration = new Configuration()
    configuration.set("fs.defaultFS", "hdfs://Spark01:9000")
    dStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .window(Seconds(5), Seconds(5))
      .reduceByKey((v1: Int, v2: Int) => v1 + v2, 1)
      //      .saveAsTextFiles("result",".txt")//保存在本地文件系统中,默认保存在程序所在的目录
      //      .saveAsNewAPIHadoopFiles("result", "", classOf[Text], classOf[IntWritable], classOf[TextOutputFormat[Text, IntWritable]], configuration)
      .foreachRDD(rdd => {
      //遍历DStream的每个分区
      rdd.foreachPartition(iter=>{
        //创建连接对象，一个分区共享一个连接对象，这样做避免了对象的重复创建和销毁
        val jedisPool = new JedisPool("Spark01",6379)

        //迭代当前分区中所有的结果集
        iter.foreach(t=>{
          val word = t._1
          val count = t._2
          val jedis = jedisPool.getResource
          jedis.set(word,count.toString)
          //连接对象使用完之后返回到连接池中
          jedisPool.returnResource(jedis)
        })
      })
    })
    //启动应用
    ssc.start()
    //优雅的关闭
    ssc.awaitTermination()
  }
}
