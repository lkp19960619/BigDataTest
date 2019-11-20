package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object FlinkWordCountsValueState {
  def main(args: Array[String]): Unit = {
    //1、创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、创建DataStream
    val dataStream = environment.socketTextStream("Spark", 9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .map(new RichMapFunction[(String, Int), (String, Int)] { //指定输入输出类型
        //执行函数可以拿到状态
        //定义一个初始化变量
        var valueState: ValueState[Int] = _

        //所有的Rich函数一般都有open方法
        override def open(parameters: Configuration): Unit = {
          //如果想拿到一个State的引用，必须创建相应StateDescriptor
          val vsd = new ValueStateDescriptor[Int]("wordcount", createTypeInformation[Int])
          //获取RuntimeContext对象，然后调用该对象的相应方法获取State对象
          valueState = getRuntimeContext.getState(vsd)
        }

        override def map(value: (String, Int)): (String, Int) = {
          //取出历史值
          var historyValue = valueState.value()
          if(historyValue==null){
            historyValue=0
          }
          //更新历史
          valueState.update(historyValue+value._2)
          (value._1,valueState.value())
        }
      })
      .print()
    environment.execute("FlinkWordCountsValueState")
  }
}
