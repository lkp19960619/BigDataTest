package com.baizhi.flink.eventtime

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkTumblingWindows03 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    //设置时间特性，如果不设置，默认使用的是ProcessingTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置水位线计算频率
    environment.getConfig.setAutoWatermarkInterval(1000)

    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark",9999)
    //定义输出标签
    val lateData = new OutputTag[(String,Long)]("late")
    //对数据做转换
    //a 时间戳
    val stream = dataStream.map(_.split("\\s+"))
      .map(ts => (ts(0), ts(1).toLong))
      .assignTimestampsAndWatermarks(new UserDefineAssignerWithPeriodicWatermarks) //计算水位线
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      //允许迟到数据小于2s，一旦水位线-窗口的结束时间大于或等于2s，再来的数据就丢弃
      .allowedLateness(Time.seconds(2)) //处理迟到数据（水位线没过窗口结束时间之后到达的数据）
      .sideOutputLateData(lateData) //将太迟的数据输出
      .apply(new AllWindowFunction[(String, Long), String, TimeWindow] {
        val format = new SimpleDateFormat("HH:mm:ss")

        override def apply(window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
          println(format.format(window.getStart) + "~" + format.format(window.getEnd))
          out.collect(input.map(t => t._1 + "->" + format.format(t._2)).reduce((v1, v2) => v1 + "|" + v2))
        }
      })
    stream.print("窗口")
    stream.getSideOutput(lateData).print("太迟的数据")

    environment.execute("FlinkWordCountsTumbingWindows")
  }
}
