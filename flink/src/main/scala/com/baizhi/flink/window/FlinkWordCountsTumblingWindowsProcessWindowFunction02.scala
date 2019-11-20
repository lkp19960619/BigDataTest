package com.baizhi.window

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkWordCountsTumblingWindowsProcessWindowFunction02 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark",9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((v1:(String,Int),v2:(String,Int))=>(v1._1,v1._2+v2._2),new ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow]{ //ProcessWindowFunction可以操作状态还可以获取所有的元数据
        override def process(key: String,
                             context: Context,  //通过context（上下文对象）可以拿到
                             elements: Iterable[(String, Int)],
                             out: Collector[(String, Int)]): Unit = {
          val w = context.window //拿到窗口元数据
          val sdf = new SimpleDateFormat("HH:mm:ss")
          println(sdf.format(w.getStart)+" ~ "+sdf.format(w.getEnd))
          val total = elements.map(_._2).sum
          out.collect((key,total))
        }
      })

      .print()
    environment.execute("FlinkWordCountsTumblingWindowsProcessWindowFunction02")
  }
}
