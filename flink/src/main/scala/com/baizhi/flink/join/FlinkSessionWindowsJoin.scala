package com.baizhi.flink.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 订单流连接用户流
 */
object FlinkSessionWindowsJoin {
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
    val userStream: DataStream[(String, String, Long)] = environment.socketTextStream("Spark", 9999)
      .map(_.split("\\s+"))
      .map(ts => (ts(0), ts(1), ts(2).toLong))
      .assignTimestampsAndWatermarks(new UserAssignerWithPunctuatedWatermarks02)

    //001 iphonex timestamp
    val orderStream: DataStream[(String, String, Long)] = environment.socketTextStream("Spark", 8888)
      .map(_.split("\\s+"))
      .map(ts => (ts(0), ts(1), ts(2).toLong))
      .assignTimestampsAndWatermarks(new OrderAssignerWithPunctuatedWatermarks)

    userStream.join(orderStream)
      .where(_._1)
      .equalTo(_._1)
      .window(EventTimeSessionWindows.withGap(Time.seconds(2)))
      .apply((v1,v2,out:Collector[String])=>{
        out.collect(v1._1+"\t"+v1._2+"\t"+v2._2)
      })
      .print()
    environment.execute("FlinkWordCountsTumbingWindows")
  }
}
