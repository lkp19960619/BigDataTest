package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object FlinkWordCountsReduceState {
  def main(args: Array[String]): Unit = {
    //1、创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、创建DataStream
    val dataStream = environment.socketTextStream("Spark", 9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .map(new RichMapFunction[(String,Int),(String,Int)] {//指定输入输出类型
        var reduceState:ReducingState[Int] = _

        override def open(parameters: Configuration): Unit = {
          //创建对应的StateDescriptor
          val rsd = new ReducingStateDescriptor[Int]("wordcount",new ReduceFunction[Int] {
            override def reduce(value1: Int, value2: Int): Int = value1+value2
          },createTypeInformation[Int])

          //获取RuntimeContext对象，调用getReducingState方法获取状态，赋值给初始变量
          reduceState = getRuntimeContext.getReducingState(rsd)
        }
        override def map(value: (String, Int)): (String, Int) = {
          reduceState.add(value._2)
          (value._1,reduceState.get())
        }
      })
      .print()
    environment.execute("FlinkWordCountsValueState")
  }
}
