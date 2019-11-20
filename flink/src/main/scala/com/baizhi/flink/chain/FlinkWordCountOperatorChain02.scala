package com.baizhi.chain

import org.apache.flink.streaming.api.scala._

object FlinkWordCountOperatorChain02 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建DataStream
    val dataStream = environment.socketTextStream("Spark",9999)
    dataStream.flatMap(_.split("\\s+"))
      .filter(lines=>lines.startsWith("INFO"))
      .map((_,1))
      .map(t=>WordPair(t._1,t._2))
      .keyBy(0)
      .sum(1)
      .print()
    environment.execute("FlinkWordCountOperatorChain02")

  }
}
