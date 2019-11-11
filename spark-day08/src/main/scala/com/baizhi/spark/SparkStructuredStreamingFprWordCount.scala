package com.baizhi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * 实时单词计数
  *
  * spark structured streaming提供了一种简化的方式（SQL）对流数据进行分析处理
  */
object SparkStructuredStreamingFprWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark structured streaming")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //spark构建流数据源
    val dataFrame = spark
      .readStream
      .format("socket") //数据源格式
      .option("host", "spark01")
      .option("port", "4444")
      .load()
    val dataSet = dataFrame.as[String].flatMap(_.split(" ")).map((_,1))
    dataSet.createOrReplaceTempView("t_word")
//    dataSet.sqlContext.sql("select _1 as word,count(_2) as total from t_word group by _1")
//    dataFrame.createOrReplaceTempView("t_word")

    //应用计算规则
    val wordcounts = spark.sql("select _1 as word,count(_2) as total from t_word group by _1")

    //输出结果
    wordcounts
      .writeStream //构建输出流
      .format("console") //输出到控制台
//      .outputMode(OutputMode.Append()) //输出模式
      .outputMode(OutputMode.Update()) //Append模式不支持聚合操作
      .start()
      .awaitTermination()
  }

  /**
    *  编程步骤：
    *  1、构建SparkSession对象（不光是SQL的入口，也是StructuredStreaming应用入口）
    *  2、通过SparkSession对象调用readStream方法构建数据流的DF对象
    *  3、数据流的DF应用SQL计算或者将DF转为DS应用计算操作
    *  4、数据流的计算结果需要DF.writeStream.format(输出格式).outputMode(输出模型)
    *  5、启动和关闭应用
    */
}
