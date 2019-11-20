package com.baizhi.flink.datasink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
object FlinkWordCountWriteAsText {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    //通过socket创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark01",9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .writeAsText("file:///d:\\result")
    environment.execute()
  }
}
