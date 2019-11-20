package com.baizhi.flink.datasink

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * 使用Redis Sink需要自定义类继承RedisMapper并实现方法
  */
class RedisExampleMapper extends RedisMapper[(String,Int)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"word-count")
  }

  override def getKeyFromData(t: (String, Int)): String = {
    t._1
  }

  override def getValueFromData(t: (String, Int)): String = {
    t._2.toString
  }
}
