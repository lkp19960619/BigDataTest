package com.baizhi.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Dataframe是一种特殊的Dataset
  */
object SparkSQLDataframeWordCountApplication {
  def main(args: Array[String]): Unit = {
    //创建SparkSession对象
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("first dataframe application")
      .getOrCreate()

    //设置控制台显示日志级别
    spark.sparkContext.setLogLevel("ERROR")

    //导入spark定义的隐式增强
    import spark.implicits._

    //创建Dataframe
    val wordRDD = spark.sparkContext
      .makeRDD(Array("Hello Kafka", "Hello Stream", "Hello Zookeeper", "Hello Spark"))
      .flatMap(_.split(" "))
      .map((_, 1))
    val df: DataFrame = wordRDD.toDF()
    df.createOrReplaceTempView("t_word")
    df.sqlContext.sql("select _1 as word,count(_2) as total from t_word group by _1").show()

    //关闭应用
    spark.stop()

  }
}
