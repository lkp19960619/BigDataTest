package com.baizhi.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkWordCountsTumblingWindows {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark",9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//      .reduce((v1,v2)=>(v1._1,v1._2+v2._2)) //在窗口操作中，聚合操作是必须的
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1,value1._2+value2._2)
        }
      })
      .print()
    environment.execute("FlinkWordCountsTumbingWindows")
  }
}
