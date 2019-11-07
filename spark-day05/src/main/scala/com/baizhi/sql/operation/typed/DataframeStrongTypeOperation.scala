package com.baizhi.sql.operation.typed

import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Dataframe的强类型Operation
  */
object DataframeStrongTypeOperation {
  def main(args: Array[String]): Unit = {
    //创建Spark Sql程序的入口->SparkSession
    val spark = SparkSession.builder().appName("dataframe strong operation application").master("local[*]").getOrCreate()

    //设置日志等级
    spark.sparkContext.setLogLevel("ERROR")

    //获取一行数据
    val lines = Array("this is a demo", "hello spark")
    //对数据进行初步切割映射
    val wordRDD = spark.sparkContext.makeRDD(lines).flatMap(_.split(" ")).map((_, 1))

    //将RDD转换成Dataset
    //首先要导入spark的依赖
    import spark.implicits._
    //转换
    val ds: Dataset[(String, Int)] = wordRDD.toDS()

    //对Dataset进行算子操作，以上操作获取到的是一个个的二元组->(this,1),(is,1),(a,1)
    ds.groupByKey(t => t._1) //根据二元组的第一个数进行分组，也就是单词
      .agg(typed.sum[(String, Int)](t => t._2).name("total")) //进行聚合操作
      .show()

    //关闭应用
    spark.stop()
  }
}
