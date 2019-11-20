package com.baizhi.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkWordCountsSlidingWindows {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark",9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(4),Time.seconds(2)))
      .fold(("",0))((z,v)=>(v._1,z._2+v._2)) //在窗口操作中，聚合操作是必须的
      .print()
    environment.execute("FlinkWordCountsSlidingWindows")
  }
}
