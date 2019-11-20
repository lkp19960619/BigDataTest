package com.baizhi.flink.datasink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * 自定义Sink必须实现SinkFunction接口或者它的子接口，并且实现接口的同时要指定返回值
  * 如果要把结果写到外部系统，要实现RichSinkFunction，带有状态的函数
  * 这种带有状态的函数可以调用生命周期方法去初始化连接
  */
class UserDefineRichSinkFunction extends RichSinkFunction[(String, Int)] {
  override def open(parameters: Configuration): Unit = {
    println("初始化连接")
  }

  override def invoke(value: (String, Int)): Unit = {
    println("具体的操作部分")
    println("insert into mysql"+value)
  }

  override def close(): Unit = {
    println("释放连接")
  }
}
