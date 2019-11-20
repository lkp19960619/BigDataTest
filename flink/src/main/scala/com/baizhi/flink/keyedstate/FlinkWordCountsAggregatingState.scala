package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object FlinkWordCountsAggregatingState {
  def main(args: Array[String]): Unit = {
    //1、创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、创建DataStream
    val dataStream = environment.socketTextStream("Spark", 9999)
    dataStream.map(_.split("\\s+"))
      .map(ts=>Employee(ts(0).toInt,ts(1),ts(2),ts(3).toDouble))
      .keyBy("dept")
      .map(new RichMapFunction[Employee,(String,Double)] {//输入的是一个Employee对象，输出的是部门和平均工资
        var aggregatingState:AggregatingState[Double,Double] = _
        override def open(parameters: Configuration): Unit = {
          //创建对应的StateDescriptor
          //方法的中间参数(Double,Int)-->(工资总数，数量总数)，表示中间变量
          val asd = new AggregatingStateDescriptor[Double, (Double, Int), Double]("aggregating", new AggregateFunction[Double, (Double, Int), Double] {
            override def createAccumulator(): (Double, Int) = (0.0, 0)

            override def add(value: Double, accumulator: (Double, Int)): (Double, Int) = {
              val total = accumulator._1
              val count = accumulator._2
              (total + value, count + 1)
            }

            override def getResult(accumulator: (Double, Int)): Double = {
              accumulator._1 / accumulator._2
            }

            override def merge(a: (Double, Int), b: (Double, Int)): (Double, Int) = {
              (a._1 + b._1, a._2 + b._2)
            }
          }, createTypeInformation[(Double, Int)])
          aggregatingState = getRuntimeContext.getAggregatingState(asd)
        }

        override def map(value: Employee): (String, Double) = {
          aggregatingState.add(value.salary)
          (value.dept,aggregatingState.get())
        }
      })
      .print()
    environment.execute("FlinkWordCountsValueState")
  }
}
