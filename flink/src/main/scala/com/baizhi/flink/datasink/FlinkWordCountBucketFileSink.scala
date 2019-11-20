package com.baizhi.flink.datasink

import java.time.ZoneId

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
  * 结果输出到外部文件系统中
  */
object FlinkWordCountBucketFileSink {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置任务并行度
    environment.setParallelism(3)
    //创建BucketingSink
    val bucketSink = new BucketingSink[String]("hdfs://Spark01:9000/bucketSink")
    //分桶sink按照系统时间对数据进行切分，并以指定的时间格式给每个桶命名
    bucketSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd-HH",ZoneId.of("Asia/Shanghai")))
    //通过socket创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark01", 9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .map(t=>t._1+"\t"+t._2)
      //会在输出的结果前面加上指定的信息
      .addSink(bucketSink)
    environment.execute("FlinkWordCountBucketFileSink")
  }
}
