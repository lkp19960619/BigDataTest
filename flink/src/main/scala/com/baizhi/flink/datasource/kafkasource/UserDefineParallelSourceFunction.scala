package com.baizhi.flink.datasource.kafkasource

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
  * 任务是并行计算的
  */
class UserDefineParallelSourceFunction extends ParallelSourceFunction[String] {
  //准备数据
  val lines = Array("this is demo", "Hello Flink", "Hello World")
  @volatile
  var isRunning = true

  //开始运行
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while (isRunning) {
      Thread.sleep(1000)
      sourceContext.collect(lines(new Random().nextInt(lines.length)))
    }
  }

  //取消任务
  override def cancel(): Unit = {
    isRunning = false
  }
}
