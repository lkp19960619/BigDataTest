package com.baizhi.flink.day02.datasource.collection

import javax.sound.midi.Sequence
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable

object FlinkWordCountCollectionSource {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val data: immutable.Seq[String] = List("this is demo", "Hello Collection", "Hello Kafka", "Hello Flink")
    val dataStream: DataStream[String] = environment.fromCollection(data)
//    environment.fromCollection(Sequence[String])
    dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()
    environment.execute()
  }
}
