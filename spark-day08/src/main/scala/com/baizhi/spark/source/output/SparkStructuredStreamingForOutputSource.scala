package com.baizhi.spark.source.output

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.OutputMode
import redis.clients.jedis.Jedis

/**
  * 如何通过不同的sink构建结构化流计算结果的输出
  * 构建结构化流计算结果的输出必须指定检查点目录
  */
object SparkStructuredStreamingForOutputSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("spark structured output application").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    //通过Kafka构建流数据输入对象
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092")
      .option("startingOffsets","""{"baizhi":{"0":-2}}""")
      .option("subscribe", "baizhi")
      .load()

    //    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(timestamp AS LONG)")
    //      .as[(String, String, String, Int, Long)]
    //
    //    ds.createOrReplaceTempView("t_kafka")
    //
    //    val text = spark.sql("select * from t_kafka")

    //---------------------------------------------------------------------------------------
    //以文件的格式进行数据 json | text | csv | parquet | orc
    //    text
    //      .writeStream
    //      .format("json")
    //      .outputMode(OutputMode.Append()) //对文件的输出模式只能是Append（追加）模式
    //      .option("checkpointLocation", "hdfs://Spark01:9000/checkpoint1")
    //      .option("path", "file:///d://result") //输出路径,path支持本地文件系统和HDFS文件系统
    //      .start()
    //      .awaitTermination()
    //---------------------------------------------------------------------------------------

    //-------------------Kafka[输出模式支持：Append | Updated | Completed]-------------------
    //    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(timestamp AS LONG)")
    //      .as[(String, String, String, Int, Long)]
    //      .flatMap(_._2.split(" "))//进行单词切割
    //      .map((_, 1))
    //
    //    ds.createOrReplaceTempView("t_kafka")
    //
    //    val text = spark.sql("select _1 as word,count(_2) as total from t_kafka group by _1")
    //    text
    ////      .selectExpr("CAST(key AS String)", "CAST(value AS String)")
    //        .selectExpr("CAST(word AS STRING) as key","CAST(total AS STRING) as value") //进行单词计数
    //      .writeStream
    //      .format("kafka")
    ////      .outputMode(OutputMode.Append())
    //      .outputMode(OutputMode.Update())
    //      .option("checkpointLocation", "hdfs://Spark01:9000/checkpoint3")
    //      .option("kafka.bootstrap.servers", "HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092") //配置输出kafka集群信息
    //      .option("topic", "result1")
    //      .start()
    //      .awaitTermination()
    //---------------------------------------------------------------------------------------
    //------------------Foreach[输出模式支持：Append | Updated | Completed]--------------------
    //将计算结果输出到redis中
    val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)", "CAST(partition AS INT)", "CAST(timestamp AS LONG)")
      .as[(String, String, String, Int, Long)]
      .flatMap(_._2.split(" ")) //进行单词切割
      .map((_, 1))

    ds.createOrReplaceTempView("t_kafka")

    val text = spark.sql("select _1 as word,count(_2) as total from t_kafka group by _1")

    text.writeStream
      .foreach(new ForeachWriter[Row] {
        /**
          *
          * @param partitionId 分区序号
          * @param epochId     数据编号
          * @return Boolean true  创建一个链接  对这一行数据进行处理操作
          *         false   不会创建链接  跳过这一行数据
          */
        override def open(partitionId: Long, epochId: Long): Boolean = true

        /**
          *
          * @param value 参数是resultTable  行对象
          */
        override def process(value: Row): Unit = {
          val word = value.getString(0)
          val total = value.getLong(1).toString
          val jedis = new Jedis("Spark01", 6379)
          jedis.set(word, total)
          jedis.close()
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (errorOrNull != null) {
            errorOrNull.printStackTrace()
          }
        }
      }) //对resultTable中的每一行记录应用写出规则
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", "hdfs://Spark01:9000/checkpoint4")
      .start()
      .awaitTermination()
    //---------------------------------------------------------------------------------------
  }
}
