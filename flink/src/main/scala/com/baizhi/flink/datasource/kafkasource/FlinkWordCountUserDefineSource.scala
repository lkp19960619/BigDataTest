package com.baizhi.flink.datasource.kafkasource

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object FlinkWordCountUserDefineSource {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建DataStream
    val dataStream = environment.addSource(new UserDefineParallelSourceFunction)
    //对数据做转换
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
    environment.execute()
  }
}
