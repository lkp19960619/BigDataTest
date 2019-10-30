package com.baizhi

import org.apache.spark.{SparkConf, SparkContext}

object RDDDataSourceWithCollection {
  def main(args: Array[String]): Unit = {
    //local[*]表示使用本地cpu所有的核
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd create")
    val sc = new SparkContext(conf)

    //Spark RDD通过集合创建的方式一
    //    //使用默认的分区数：是当前cpu cores
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5))
    //    //通过rdd里面各种计算算子进行计算
    //    val sum = rdd.reduce(((e1, e2) => e1 + e2))
    //    println(s"$sum")
    //Spark RDD通过集合创建的方式二
    val rdd = sc.parallelize(List(4, 5, 6, 7, 8))
    val sum = rdd.reduce((e1, e2) => e1 + e2)
    println(sum)
    //打印当前rdd的分区数量
    println(rdd.getNumPartitions)
    //停止spark程序
    sc.stop()
  }
}
