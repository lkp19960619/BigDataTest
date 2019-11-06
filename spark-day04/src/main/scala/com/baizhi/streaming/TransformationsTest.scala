package com.baizhi.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * 测试Spark Streaming的转换算子
  */
object TransformationsTest {
  def main(args: Array[String]): Unit = {
    //构建Spark Streaming的上下文对象StreamingContext
    val conf = new SparkConf().setMaster("local[*]").setAppName("transformations application")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置控制台显示的日志级别
    ssc.sparkContext.setLogLevel("ERROR")
    //创建DStream(离散数据流)
    //    val dStream = ssc.socketTextStream("Spark01", 7777)


    //操作算子的使用
    //    dStream
    //      .flatMap(str => str.split(" ")) //str表示监视到的网络端口输入的一行数据,并将它们按照空行拆分
    //      .repartition(5)//动态调整分区数量
    //      .map(word => (word, 1)) //将一种类型转换为另一种类型
    //      .filter(t => !t._1.equals("Hello")) //保留符合条件的结果
    //      .reduceByKey((v1, v2) => v1 + v2) //将key相同的两个相邻的value进行相加计算
    //      .print()

    //-----------------------------------------------------------------------------
    //union 联合结合  将两个DStream内的元素合并为一个DStream
    //    val dStream1 = ssc.socketTextStream("Spark01", 7777)
    //    val dStream2 = ssc.socketTextStream("Spark01", 8888)
    //    dStream1.union(dStream2).print()

    //-----------------------------------------------------------------------------
    //    val dStream3 = ssc.socketTextStream("Spark01",7777)
    //    dStream3.count()  //返回微批中RDD元素的个数
    //      .print()

    //-----------------------------------------------------------------------------
    //    val dStream4 = ssc.socketTextStream("Spark01", 7777)
    //    dStream4
    //      .map(strNum => strNum.toInt)
    //      .reduce((v1, v2) => v1 + v2) //reduce使用的时候，相邻的两个元素需要支持数学运算
    //      .print()

    //-----------------------------------------------------------------------------
    //countByValue：统计一个微批中同一个元素出现的个数
    //    val dStream5 = ssc.socketTextStream("Spark01",7777)
    //    dStream5
    //        .flatMap(_.split(" "))
    //        .countByValue()
    //        .print()

    //-----------------------------------------------------------------------------
    //join：(k,v)和(k,w)返回(k,(v,w)),k相同
    //    val dStream6 = ssc.socketTextStream("Spark01",7777).flatMap(_.split(" ")).map((_,1))
    //    val dStream7 = ssc.socketTextStream("Spark01",8888).flatMap(_.split(" ")).map((_,1))
    //    dStream6.join(dStream7).print()

    //-----------------------------------------------------------------------------
    //cogroup 两个DStream连接的 返回一个(k,Seq[v],seq[w])
    //    val dStream8 = ssc.socketTextStream("Spark01",4444).flatMap(_.split(" ")).map((_,1))
    //    val dStream9 = ssc.socketTextStream("Spark01",5555).flatMap(_.split(" ")).map((_,1))
    //    dStream8.cogroup(dStream9).print()

    //-----------------------------------------------------------------------------
    //transform 将DStream转换为RDD处理 处理完成之后返回一个DStream
    //需求：开发一个抽奖系统，要求黑名单的用户不允许抽奖
    //首先：不断产生抽奖请求(白名单+黑名单) Stream
    //其次：黑名单用户(batch)
    //Stream + Batch
    //最后：只保留白名单用户的抽奖请求

    //    val requestStream = ssc.socketTextStream("Spark01", 5555)
    //
    //    //声明黑名单
    //    val blackList = List(("001", "zs"), ("002", "ls"), ("003", "ww"))
    //    //把黑名单变成一个批次数据
    //    //1、获取SparkContext对象
    //    val sc = ssc.sparkContext
    //    //2、批次处理
    //    val blackListRDD = sc.makeRDD(blackList)
    //
    //    //对流数据的格式进行转换
    //    requestStream
    //      .map(line => {
    //        val arr = line.split(" ")
    //        val userId = arr(0)
    //        val requestUrl = arr(1)
    //        //返回一个二元组
    //        (userId, requestUrl)
    //      })
    //      //把流数据转换成批数据
    //      .transform(mapTransformRDD => {
    //      mapTransformRDD
    //        .leftOuterJoin(blackListRDD) //对转换后的RDD和黑名单的RDD进行左外链接
    //        .filter(t=>t._2._2.isEmpty)   //判断连接黑名单之后是否有值
    //    })
    //      .print()

    //-----------------------------------------------------------------------------
    //updateStateByKey
    //spark streaming中所有的状态计算，必须设置检查点目录
    //    ssc.checkpoint("hdfs://Spark01:9000/checkpoint2")
    //    val dStream10 = ssc.socketTextStream("Spark01", 5555)
    //    dStream10
    //      .flatMap(_.split(" "))
    //      .map((_, 1))
    //      //状态数据 k  v
    //      //values：代表微批RDD中key相同的value集合
    //      //state：累积的状态数据
    //      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {//updateStateByKey是一种全量更新，不管状态有没有改变都会打印
    //      Some(values.size + state.getOrElse(0))
    //    }) //返回一个带状态的DStream
    //      .print()

    //-----------------------------------------------------------------------------
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
        }else{
          //不存在，赋予初值1
          count = v.getOrElse(1)
        }
        //将最新的计算结果更新到状态中
        state.update(count)
        (k,count)//将状态更新后的DStream传递给下游处理
      }))//mapWithState是状态数据的增量输出，状态不改变就不会输出
        .print()
    //这样做重新启动应用之后状态数据并不会恢复，原因是只设定了状态数据的存储位置，却并没有通过状态数据恢复程序


    //启动应用
    ssc.start()
    //优雅的关闭
    ssc.awaitTermination()
  }
}
