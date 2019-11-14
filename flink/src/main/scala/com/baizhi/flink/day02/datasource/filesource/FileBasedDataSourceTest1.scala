package com.baizhi.flink.day02.datasource.filesource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * readTextFile()
  */
object FileBasedDataSourceTest1 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    //创建DataStream
    val dataStream = environment.readTextFile("file:///d:\\data")
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(t=>t._1)
      .sum(1)
      .print()
    environment.execute()
  }

}
