package com.baizhi

import org.apache.spark.{SparkConf, SparkContext}

object RDDCheckPoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rdd cache demo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //从本地文件系统中读取一个文件
    val rdd = sc.textFile("file:///d://testdata/*.txt")

    sc.setCheckpointDir("hdfs://Spark01:9000/checkpoint")
    rdd.checkpoint()

    //行动算子count获取数据行的数量
    rdd.count()

    //记时
    val start = System.currentTimeMillis()
    rdd.count()
    val end = System.currentTimeMillis()
    println("使用检查点，计算耗时ms：" + (end - start))

    //清空缓存
    rdd.unpersist()
    val start2 = System.currentTimeMillis()
    rdd.count()
    val end2 = System.currentTimeMillis()
    println("没有使用检查点，计算耗时ms：" + (end2 - start2))

    //关闭spark应用
    sc.stop()
  }
}
