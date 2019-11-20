package com.baizhi.chain

import org.apache.flink.streaming.api.scala._

/**
 * startNewChain()用于将filter和第一个mapper映射断开
 */
object FlinkWordCountOperatorChain03 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment


    //创建DataStream
    val dataStream = environment.socketTextStream("Spark",9999)
    dataStream.flatMap(_.split("\\s+"))
      .filter(lines=>lines.startsWith("INFO"))
      .startNewChain()
      .map((_,1))
      .map(t=>WordPair(t._1,t._2))
      .keyBy(0)
      .sum(1)
      .print()
    environment.execute("FlinkWordCountOperatorChain03")

  }
}
