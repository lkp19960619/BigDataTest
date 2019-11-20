package com.baizhi.flink.quickstart

import org.apache.flink.streaming.api.scala._
/**
  * Apache Flink 快速入门程序
  */
object FlinkWordCountQuickStart {
  def main(args: Array[String]): Unit = {
    //1、创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //2、创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark01",9999)

    //3、对接收到的数据进行转换
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
//    environment.execute("FlinkWordCountQuickStart")

    //打印任务的执行计划
    val plan = environment.getExecutionPlan
    println(plan)
  }
}
