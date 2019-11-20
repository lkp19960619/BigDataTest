package com.baizhi.window

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkWordCountsSessionWindows {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark",9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .aggregate(new AggregateFunction[(String,Int),(String,Int),(String,Int)] {
        override def createAccumulator(): (String, Int) = {
          ("",0)
        }

        override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = {
          (value._1,value._2+accumulator._2)
        }

        override def getResult(accumulator: (String, Int)): (String, Int) = {
          accumulator
        }

        override def merge(a: (String, Int), b: (String, Int)): (String, Int) = {
          (a._1,a._2+b._2)
        }
      }) //在窗口操作中，聚合操作是必须的
      .print()
    environment.execute("FlinkWordCountsSessionWindows")
  }
}
