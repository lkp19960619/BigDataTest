package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConverters._

/**
 * 把一个用户密码的历史信息存储到List中
 */
object FlinkWordCountsListState {
  def main(args: Array[String]): Unit = {
    //1、创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、创建DataStream
    val dataStream = environment.socketTextStream("Spark", 9999)
    dataStream.map(_.split("\\s+"))
      .map(ts => (ts(0), ts(1)))
      .keyBy(0)
      .map(new RichMapFunction[(String, String), String] { //输入的是一个用户，输出的是用户的密码
        //把用户的历史密码存储到ListState中
        var historyPasswords: ListState[String] = _

        override def open(parameters: Configuration): Unit = {
          //创建对应的StateDescriptor
          val lsd = new ListStateDescriptor[String]("liststate", createTypeInformation[String])
          //获取对应的RuntimeContext对象
          historyPasswords = getRuntimeContext.getListState(lsd)
        }

        override def map(value: (String, String)): String = {
          //拿到历史密码的集合
          var list = historyPasswords.get().asScala.toList
          //把每一次拿到的密码放入历史密码集合
          list.::(value._2)
          //去除list中重复的密码
          list = list.distinct
          //更新list
          historyPasswords.update(list.asJava)
          //拼接密码，输出对应用户的全部历史密码
          value._1 + "\t" + list.mkString(",")
        }
      })
      .print()
    environment.execute("FlinkWordCountsValueState")
  }
}
