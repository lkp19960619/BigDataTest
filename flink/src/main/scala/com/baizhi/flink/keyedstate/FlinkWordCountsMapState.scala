package com.baizhi.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConverters._

/**
 * 判断用户登录状态
 */
object FlinkWordCountsMapState {
  def main(args: Array[String]): Unit = {
    //1、创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //2、创建DataStream
    val dataStream = environment.socketTextStream("Spark", 9999)
    dataStream.map(_.split("\\s+"))
      .map(ts=>Login(ts(0),ts(1),ts(2),ts(3),ts(4)))
      .keyBy("id","name")//根据id和name分组
      .map(new RichMapFunction[Login,String] {
        var mapState:MapState[String,String] = _

        override def open(parameters: Configuration): Unit = {
          val msd = new MapStateDescriptor[String,String]("mapstate",createTypeInformation[String],createTypeInformation[String])
          mapState =getRuntimeContext.getMapState(msd)
        }
        override def map(value: Login): String = {
          println("历史登录")
          for (k <- mapState.keys().asScala) {//遍历mapState，获取登录信息
            println(k+"\t"+mapState.get(k))
          }
          //定义一个初始变量用于记录登录结果
          var result = ""
          //用户第一次登录
          if (mapState.keys().iterator().asScala.isEmpty) {
            result = "ok"
          }else{
            if (!value.address.equalsIgnoreCase(mapState.get("address"))) {//用户的登录地址和历史登录地址不符
              result = "error"
            }else{
              result = "ok"
            }
          }
          mapState.put("ip",value.ip)
          mapState.put("address",value.address)
          mapState.put("date",value.date)
          result
        }
      })
      .print()
    environment.execute("FlinkWordCountsMapState")
  }
}
