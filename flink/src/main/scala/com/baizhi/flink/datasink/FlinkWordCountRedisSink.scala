package com.baizhi.flink.datasink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

object FlinkWordCountRedisSink {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(3)
    //Redis的配置信息
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("Spark01")
      .setPort(6379)
      .build()
    //通过socket创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark01", 9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .addSink(new RedisSink[(String, Int)](conf, new RedisExampleMapper))

    environment.execute()
  }
}
