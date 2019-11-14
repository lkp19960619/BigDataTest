package com.baizhi.flink.day01

import org.apache.flink.streaming.api.scala._

/**
  * Apache Flink 快速入门程序
  */
object FlinkWordCountQuickStart03 {
  def main(args: Array[String]): Unit = {
    val jarFiles = "flink\\target\\flink-1.0-SNAPSHOT-jar-with-dependencies.jar"
    //1、创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.createRemoteEnvironment("Spark01",8081,jarFiles)

    environment.setParallelism(2)

    //2、创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark01",9999)

    //3、对接收到的数据进行转换
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
    environment.execute("FlinkWordCountQuickStart03")
  }
}
