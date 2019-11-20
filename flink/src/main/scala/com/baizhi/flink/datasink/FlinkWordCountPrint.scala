package com.baizhi.flink.datasink

import org.apache.flink.api.java.io.CsvOutputFormat
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object FlinkWordCountPrint {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(3)
    //通过socket创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark01", 9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .map(t => new Tuple2(t._1, t._2))
      //会在输出的结果前面加上指定的信息
      .writeUsingOutputFormat(new CsvOutputFormat[Tuple2[String, Int]](new Path("file:///d:\\result")))
    environment.execute()
  }
}
