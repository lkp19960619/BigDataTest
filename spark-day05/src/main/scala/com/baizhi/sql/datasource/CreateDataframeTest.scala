package com.baizhi.sql.datasource

import org.apache.spark.sql.SparkSession

/**
  * 创建Dataframe
  */
object CreateDataframeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("creta dataframe").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //通过json创建
    //    val dataFrame = spark.read.json("file:///D:\\framework_code\\BigDataTest\\spark-day05\\src\\main\\resources")
    //    //打印Datafram表格的结构
    //    dataFrame.printSchema()
    //    dataFrame.show()

    //通过样例类创建
    //    val personList = List(Person(1,"zs",23),Person(2,"ls",36),Person(3,"ww",15))
    //    //导入spark隐式增强
    //    import spark.implicits._
    //    //转换为Dataframe
    //    val dataFrame = personList.toDF()
    //    dataFrame.show()
    val tupleList = List((1,"zs",23),(2,"ls",36),(3,"ww",15))
    val dataFrame = tupleList.toDF()
    dataFrame
        .withColumnRenamed("_1","id")
        .withColumnRenamed("_2","name")
        .withColumnRenamed("_3","age")
      .show()
    //通过Tuple创建


    spark.stop()
  }
}

case class Person(id: Int, name: String, age: Int)