package com.baizhi.sql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Dataset是一种特殊的RDD
  */
object SparkSQLDatasetWordCountApplication {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession，它是SparkSql程序的入口，内嵌了SparkContext和StreamingContext
    val spark = SparkSession
      .builder()
      .appName("first spark sql application")
      .master("local[*]")
      .getOrCreate()

    //2.导入spark定义隐式增强|转换
    import spark.implicits._

    //3.创建Dataset
    //添加数据
    val lines = Array("this is a demo", "hello spark sql", "hello spark streaming")
    val wordRDD = spark.sparkContext.makeRDD(lines)
      .flatMap(_.split(" "))
      .map((_, 1))
    //(this,1),(is,1)
    val ds: Dataset[(String, Int)] = wordRDD.toDS()

    /*
      输出格式
      +-----+-------+
      | word|   num |
      +-----+-------+
      |hello|      2|
      |   is|      1|
      |  sql|      1|
      |spark|      2|
      | demo|      1|
      |    a|      1|
      | this|      1|
      +-----+-------+
     */
    //对Dataset执行sql算子操作
    ds
      .groupBy("_1")
      .sum("_2")
      .withColumnRenamed("_1","word")
      .withColumnRenamed("sum(_2)","num")
      .show()

    //关闭SparkSession
    spark.stop()
  }
}
