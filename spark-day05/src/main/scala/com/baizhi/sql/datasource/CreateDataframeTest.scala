package com.baizhi.sql.datasource

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

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

    //通过Tuple创建
    //    val tupleList = List((1,"zs",23),(2,"ls",36),(3,"ww",15))
    //    val dataFrame = tupleList.toDF()
    //    dataFrame
    //        .withColumnRenamed("_1","id")
    //        .withColumnRenamed("_2","name")
    //        .withColumnRenamed("_3","age")
    //      .show()

    //通过RDD[Row]创建Dataframe

    //返回一个Row类型的rdd
    //    val rdd = spark.sparkContext.parallelize(List((1, "Huxz", 23), (2, "Liucy", 29), (3, "Suns", 45))).map(t => Row(t._1, t._2, t._3))
    //    //创建表的结构对象
    //    val schema = new StructType()
    //      .add("id", IntegerType)
    //      .add("name", StringType)
    //      .add("age", IntegerType)
    //
    //    val dataFrame = spark.createDataFrame(rdd, schema)
    //    dataFrame.show()

    //通过Java Bean创建
    val animalList = List(new Animal(1,"Cat"),new Animal(2,"Dog"),new Animal(3,"Pig"))
    val animalRDD = spark.sparkContext.makeRDD(animalList)
    val dataFrame = spark.createDataFrame(animalRDD,classOf[Animal])
    dataFrame.show()
    spark.stop()
  }
}

case class Person(id: Int, name: String, age: Int)