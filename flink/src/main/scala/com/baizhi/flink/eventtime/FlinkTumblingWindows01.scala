package com.baizhi.flink.eventtime

import java.lang
import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkTumblingWindows01 {
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
    //对数据做转换
    //a 时间戳
    dataStream.map(_.split("\\s+"))
      .map(ts=>(ts(0),ts(1).toLong))
      .assignTimestampsAndWatermarks(new UserDefineAssignerWithPeriodicWatermarks) //计算水位线
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new AllWindowFunction[(String,Long),String,TimeWindow] {
        val format = new SimpleDateFormat("HH:mm:ss")
        override def apply( window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
          println(format.format(window.getStart)+"~"+format.format(window.getEnd))
          out.collect(input.map(t=>t._1+"->"+format.format(t._2)).reduce((v1,v2)=>v1+"|"+v2))
        }
      })
      .print()
    environment.execute("FlinkWordCountsTumbingWindows")
  }
}
