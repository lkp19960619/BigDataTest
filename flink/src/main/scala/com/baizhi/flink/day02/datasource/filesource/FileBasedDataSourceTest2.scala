package com.baizhi.flink.day02.datasource.filesource

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * readTextFile()
  */
object FileBasedDataSourceTest2 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    //创建DataStream
    val dataStream = environment.readFile(new TextInputFormat(null),"file:///d:\\data")
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(t=>t._1)
      .sum(1)
      .print()
    environment.execute()
  }

}
