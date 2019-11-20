package com.baizhi.flink.evictor

import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object FlinkWordCountsGlobalWindowsDeltaTrigger01 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark",9999)
    dataStream
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of(5))//GlobalWindow窗口想要触发需要指定一个触发策略
      .evictor(new ErrorEvictor[GlobalWindow]("error"))
      .apply(new AllWindowFunction[String,String,GlobalWindow] {
        override def apply(window: GlobalWindow, input: Iterable[String], out: Collector[String]): Unit = {
          input.foreach(t=>out.collect(t))
        }
      }) //在窗口操作中，聚合操作是必须的
      .print()
    environment.execute("FlinkWordCountsGlobalWindowsDeltaTrigger")
  }
}
