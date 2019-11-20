package com.baizhi.window

import java.lang

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object FlinkWordCountsGlobalWindows {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark",9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(_._1) //WindowFunction是一个老版的用法
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(5))//GlobalWindow窗口想要触发需要指定一个触发策略
      .apply(new WindowFunction[(String,Int),(String,Int),String,GlobalWindow] {
        override def apply(key: String, window: GlobalWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          println("key:"+key+" :w"+window)
          input.foreach(t=>println(t))
          out.collect((key,input.map(_._2).sum))
        }
      }) //在窗口操作中，聚合操作是必须的
      .print()
    environment.execute("FlinkWordCountsGlobalWindows")
  }
}
