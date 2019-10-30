package com.baizhi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算本地文件系统中的数据
  */
object RDDDataSourceWithFileSystem {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rdd filesystem").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //1、通过本地文件系统创建RDD（弹性分布式数据集对象）
    //    val rdd: RDD[String] = sc.textFile("file:///d:\\testdata\\*.txt")
    //2、读取hdfs中的文件
    //    val rdd: RDD[String] = sc.textFile("hdfs://Spark01:9000/test.txt")

    //    rdd.foreach(println)
    //    rdd.flatMap(_.split(" "))
    //      //映射成单词元组并赋予每个单词初始值1
    //      .map((_, 1))
    //      //根据单词分组
    //      .groupBy(_._1)
    //      .map(t => (t._1, t._2.size))
    //      .sortBy(_._1, true, 1)
    //      .foreach(println)

    //方式二 Tuple2(文件地址,文件内容)
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("file:///d:\\testdata\\*.txt")
    rdd.flatMap(_._2.split("\r\n"))
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(_._1)
      .map(t => (t._1, t._2.size))
      .sortBy(_._2, false)
      .foreach(println)

    //默认的分区数和文件的数量是对应的
    //    println(rdd.getNumPartitions)

    sc.stop()
  }
}
