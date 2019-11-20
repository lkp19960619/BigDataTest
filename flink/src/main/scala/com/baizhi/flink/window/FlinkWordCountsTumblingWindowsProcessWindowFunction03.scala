package com.baizhi.window

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkWordCountsTumblingWindowsProcessWindowFunction03 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark", 9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((v1: (String, Int), v2: (String, Int)) => (v1._1, v1._2 + v2._2), new ProcessWindowFunction[(String, Int), String, String, TimeWindow] { //ProcessWindowFunction可以操作状态还可以获取所有的元数据
        var windowStateDescriptor: ReducingStateDescriptor[Int] = _
        var globalStateDescriptor: ReducingStateDescriptor[Int] = _

        override def open(parameters: Configuration): Unit = {
          windowStateDescriptor = new ReducingStateDescriptor[Int]("wcs", new ReduceFunction[Int] {
            override def reduce(value1: Int, value2: Int): Int = value1 + value2
          }, createTypeInformation[Int])
          globalStateDescriptor = new ReducingStateDescriptor[Int]("gcs", new ReduceFunction[Int] {
            override def reduce(value1: Int, value2: Int): Int = value1 + value2
          }, createTypeInformation[Int])

        }

        override def process(key: String,
                             context: Context, //通过context（上下文对象）可以拿到
                             elements: Iterable[(String, Int)],
                             out: Collector[String]): Unit = {
          val w = context.window //拿到窗口元数据
          val sdf = new SimpleDateFormat("HH:mm:ss")
          println(sdf.format(w.getStart) + " ~ " + sdf.format(w.getEnd))
          val windowState = context.windowState.getReducingState(windowStateDescriptor)
          val globalState = context.globalState.getReducingState(globalStateDescriptor)
          elements.foreach(t => {
            windowState.add(t._2)
            globalState.add(t._2)
          })
          out.collect(key+"\t"+windowState.get()+"\t"+globalState.get())
        }
      })

      .print()
    environment.execute("FlinkWordCountsTumblingWindowsProcessWindowFunction03")
  }
}
