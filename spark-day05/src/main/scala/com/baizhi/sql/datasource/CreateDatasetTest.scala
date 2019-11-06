package com.baizhi.sql.datasource

import org.apache.spark.sql.SparkSession

/**
  * 测试创建Dataset的方法
  */
object CreateDatasetTest {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark = SparkSession.builder().appName("create dataset").master("local[*]").getOrCreate()
    //设置控制台显示的日志等级
    spark.sparkContext.setLogLevel("ERROR")

    //使用样例类的方式创建
    //数据
    //    val userList = List(User(1,"zs",true),User(2,"ls",false),User(3,"ww",true))
    //    //导入spark的隐式增强
    //    import spark.implicits._
    //    //创建Dataset
    //    val dataset = userList.toDS().show()

    //根据Tuple创建
    //数据
    //    val tupleList = List((1, "zs", true), (2, "ls", false), (3, "ww", true))
    //    //导入spark的隐式增强
    //    import spark.implicits._
    //    //创建Dataset
    //    val dataset = tupleList
    //      .toDS()
    //      .withColumnRenamed("_1", "id")
    //      .withColumnRenamed("_2", "name")
    //      .withColumnRenamed("_3", "sex")
    //      .show()

    //基于json创建Dataset
    val dataset = spark.read.json("file:///D:\\framework_code\\BigDataTest\\spark-day05\\src\\main\\resources").as("user")
    dataset.show()

    //停止应用
    spark.stop()
  }
}

case class User(id: Int, name: String, sex: Boolean)
