package com.baizhi.chain

import org.apache.flink.streaming.api.scala._

/**
 * disableChaining()用于和后面断开
 */
object FlinkWordCountOperatorChain04 {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment


    //创建DataStream
    val dataStream = environment.socketTextStream("Spark",9999)
    dataStream.flatMap(_.split("\\s+"))
      .filter(lines=>lines.startsWith("INFO"))
      .map((_,1))
      .disableChaining()//它之前的一个算子既不和前面连接又不和后面连接
      .map(t=>WordPair(t._1,t._2))
      .keyBy(0)
      .sum(1)
      .print()
    environment.execute("FlinkWordCountOperatorChain04")

  }
}
