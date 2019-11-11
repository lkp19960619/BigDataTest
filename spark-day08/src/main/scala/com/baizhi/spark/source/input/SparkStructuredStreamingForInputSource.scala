package com.baizhi.spark.source.input

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType}

/**
  * 如何通过不同的source构建流数据源
  */
object SparkStructuredStreamingForInputSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark structured streaming application")
      .getOrCreate()

    //设置控制台显示日志级别
    spark.sparkContext.setLogLevel("ERROR")

    //导入spark的隐式增强
    import spark.implicits._

    //---------------------------------------------------------------------
    //通过file的形式构建流数据源
    //val df = spark.readStream.textFile("file:///d:\\data")
    //---------------------------------------------------------------------

    //---------------------------------------------------------------------
    //通过json的方式构建流数据源,通过hdfs分布式文件系统上的json文件构建流数据源
    //    val df = spark
    //      .readStream
    //      .format("json")
    //      .schema(new StructType() //json | csv | parquet | orc文件都需要指定表的结构
    //        .add("id", IntegerType)
    //        .add("name", StringType)
    //        .add("sex", BooleanType))
    //      .load("hdfs://Spark01:9000/json")
    //---------------------------------------------------------------------

    //---------------------------------------------------------------------
    //通过Kafka构建流数据对象（重点）
    val df = spark
      .readStream
      .format("kafka")
        .option("kafka.bootstrap.servers","HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092") //添加Kafka集群配置信息
        .option("startingOffsets","""{"baizhi":{"0":-2}}""")//从百知主题的0hao分区以earliest的方式消费(-1表示latest，-2表示earliest)
        .option("subscribe","baizhi")
        .load()

    //将主题中的一条记录的key和value转化为指定类型
    val ds = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(timestamp AS LONG)")
      .as[(String, String, String, Int, Long)]

    ds.createOrReplaceTempView("t_kafka")

    //---------------------------------------------------------------------
    //创建表
//    df.createOrReplaceTempView("t_text")

    //相关操作
    val text = spark.sql("select * from t_kafka")

    //输出
    text
      .writeStream
      .format("console") //在控制台打印
      .outputMode(OutputMode.Append())
      .start()
      .awaitTermination()
  }
}
