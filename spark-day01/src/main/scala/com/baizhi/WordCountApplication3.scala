package com.baizhi

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 开发Spark应用   在yarn上面运行
  */
object WordCountApplication3 {
  def main(args: Array[String]): Unit = {
    //1、初始化SparkConf和SparkContext对象
    val conf = new SparkConf().setAppName("wordCount").setMaster("yarn")
    val sc = new SparkContext(conf)

    //2、进行大数据集的批处理
    sc.textFile("hdfs://Spark01OnYarn:9000/test.txt")
      .flatMap(_.split(" ")) //拆分成单词
      .map((_, 1)) //映射成元组，并且给每个单词赋予初值1
      .groupBy(_._1) //元组中的第一个单词分组
      .map(t => (t._1, t._2.size))
      .sortBy(_._2,false,1)//根据单词出现的次数，进行降序排列
      .saveAsTextFile("hdfs://Spark01OnYarn:9000/result1")

    //3、释放资源
    sc.stop()
  }
}
