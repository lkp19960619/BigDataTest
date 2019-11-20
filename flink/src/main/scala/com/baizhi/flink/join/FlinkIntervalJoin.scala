package com.baizhi.flink.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 订单流连接用户流
 */
object FlinkIntervalJoin {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    //设置时间特性，如果不设置，默认使用的是ProcessingTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置水位线计算频率
    environment.getConfig.setAutoWatermarkInterval(1000)
    //创建DataStream
    //001 zhangsan  timestamp
    val userKeyedStream: KeyedStream[(String, String, Long), String] = environment.socketTextStream("Spark", 9999)
      .map(_.split("\\s+"))
      .map(ts => (ts(0), ts(1), ts(2).toLong))
      .assignTimestampsAndWatermarks(new UserAssignerWithPunctuatedWatermarks02)
      .keyBy(t => t._1)
    //001 iphonex timestamp
    val orderKeyedStream: KeyedStream[(String, String, Long), String] = environment.socketTextStream("Spark", 8888)
      .map(_.split("\\s+"))
      .map(ts => (ts(0), ts(1), ts(2).toLong))
      .assignTimestampsAndWatermarks(new OrderAssignerWithPunctuatedWatermarks)
      .keyBy(t => t._1)
    userKeyedStream.intervalJoin(orderKeyedStream)
      .between(Time.seconds(-2), Time.seconds(2))
      .process(new ProcessJoinFunction[(String, String, Long), (String, String, Long), String] {
        override def processElement(left: (String, String, Long),
                                    right: (String, String, Long),
                                    ctx: ProcessJoinFunction[(String, String, Long), (String, String, Long), String]#Context,
                                    out: Collector[String]): Unit = {
          val leftTimestamp = ctx.getLeftTimestamp
          val rightTimestamp = ctx.getRightTimestamp
          val timestamp = ctx.getTimestamp
          println(s"left:$leftTimestamp,right:$rightTimestamp,timestamp:$timestamp")
          out.collect(left._1 + "\t" + left._2 + "\t" + right._2)
        }
      })
      .print()
    environment.execute("FlinkWordCountsTumbingWindows")
  }
}
