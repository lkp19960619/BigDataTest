package com.baizhi.windows

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 需求：每隔5秒统计最近10秒产生的数据
  */
object WindowsTransformationsTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first windows application").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    //设置检查点目录，窗口计算必须设置检查点目录
    ssc.checkpoint("hdfs://Spark01:9000/checkpoint5")
    val dStream = ssc.socketTextStream("Spark01", 5555)
    //    dStream
    //      .flatMap(_.split(" "))
    //      .map((_,1))
    //      .countByWindow(Seconds(5),Seconds(5))
    //      .print()

    dStream
      .flatMap(_.split(" "))
      .map((_, 1))
      //增量计算，上一个窗口的计算结果+当前窗口的新入内容-上一个窗口的移除元素
      .reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, (v1: Int, v2: Int) => v1 - v2, Seconds(5), Seconds(3))
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
