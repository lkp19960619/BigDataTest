package com.baizhi.windows

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 需求：每隔5秒统计最近10秒产生的数据
  */
object WindowsTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first windows application").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    val dStream = ssc.socketTextStream("Spark01",5555)
    dStream
      .flatMap(_.split(" "))
      .map((_,1))
      .window(Seconds(10),Seconds(5))//窗口长度是10s，滑动步长是5s，跳跃滑动窗口
      .reduceByKey(_+_)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
