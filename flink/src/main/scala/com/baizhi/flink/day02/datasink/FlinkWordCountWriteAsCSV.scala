package com.baizhi.flink.day02.datasink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object FlinkWordCountWriteAsCSV {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    //通过socket创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark01", 9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .writeAsCsv("file:///d:\\result")
    environment.execute()
  }
}
