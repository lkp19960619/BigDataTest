package com.baizhi.flink.day02.datasink

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

class UserDefineKafkaSink extends KeyedSerializationSchema[(String,Int)]{
  override def serializeKey(t: (String, Int)): Array[Byte] = t._1.getBytes

  override def serializeValue(t: (String, Int)): Array[Byte] = t._2.toString.getBytes

  override def getTargetTopic(t: (String, Int)): String = "topic01"
}
