package com.baizhi

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDDCache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rdd cache demo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //从本地文件系统中读取一个文件
    val rdd = sc.textFile("file:///d://testdata/*.txt")
    //尝试对RDD进行cache,默认存到内存中
//    rdd.cache()
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    //行动算子count获取数据行的数量
    rdd.count()

    //记时
    val start = System.currentTimeMillis()
    rdd.count()
    val end = System.currentTimeMillis()
    println("使用缓存cache，计算耗时ms：" + (end - start))

    //清空缓存
    rdd.unpersist()
    val start2 = System.currentTimeMillis()
    rdd.count()
    val end2 = System.currentTimeMillis()
    println("不使用缓存cache，计算耗时ms：" + (end2 - start2))

    //关闭spark应用
    sc.stop()
  }
}
