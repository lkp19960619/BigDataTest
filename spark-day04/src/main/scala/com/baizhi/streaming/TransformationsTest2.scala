package com.baizhi.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * 测试带状态的数据的恢复
  */
object TransformationsTest2 {
  def main(args: Array[String]): Unit = {
    //SparkStreaming在进行状态恢复时，需要通过getOrCreate方式初始化ssc
    val ssc = StreamingContext.getOrCreate("hdfs://Spark01:9000/checkpoint3", () => {
      //构建Spark Streaming的上下文对象StreamingContext
      val conf = new SparkConf().setMaster("local[*]").setAppName("transformations application")
      val ssc = new StreamingContext(conf, Seconds(5))

      //mapWithState 带状态的操作算子
      ssc.checkpoint("hdfs://Spark01:9000/checkpoint3")
      val dStream11 = ssc.socketTextStream("Spark01", 5555)
      dStream11
        .flatMap(_.split(" "))
        .map((_, 1))
        .mapWithState(StateSpec.function((k: String, v: Option[Int], state: State[Int]) => {
          var count = 0
          //首先判断状态数据是否存在
          if (state.exists()) {
            //如果存在，在历史的状态基础上增加
            count = state.get() + v.get
          } else {
            //不存在，赋予初值1
            count = v.getOrElse(1)
          }
          //将最新的计算结果更新到状态中
          state.update(count)
          (k, count) //将状态更新后的DStream传递给下游处理
        })) //mapWithState是状态数据的增量输出，状态不改变就不会输出
        .print()
      ssc
    })
    //设置控制台显示的日志级别
    ssc.sparkContext.setLogLevel("ERROR")

    //启动应用
    ssc.start()
    //优雅的关闭
    ssc.awaitTermination()
  }
}
