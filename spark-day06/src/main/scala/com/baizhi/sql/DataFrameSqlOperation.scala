package com.baizhi.sql

import org.apache.spark.sql.SparkSession

/**
  * Dataframe的操作
  */
object DataFrameSqlOperation {
  def main(args: Array[String]): Unit = {
    //创建SparkSession，它是Spark Sql程序的入口，内含SparkContext和SparkStreaming
    val spark = SparkSession.builder().appName("dataframe strong typed application").master("local[*]").getOrCreate()

    //设置显示日志等级
    spark.sparkContext.setLogLevel("ERROR")

    //创建RDD
    val rdd = spark.sparkContext.makeRDD(List("Hello Hadoop","Hello Scala","Hello Spark Sql")).flatMap(_.split(" ")).map((_,1))

    //导入spark的隐式增强
    import spark.implicits._

    //rdd->df
    val dataFrame = rdd.toDF("word","num")

    //全局表的访问需要加前缀->global_temp.
    dataFrame.createGlobalTempView("t_user") //对DF创建全局的临时视图，它产生的表，可以多个spark session共享，它的生命周期和spark application绑定
    //dataFrame.createTempView() //对DF创建局部的临时视图，它产生的表，仅可在创建它的spark session中使用，其它的spark session无法获取
    spark.sql("select * from global_temp.t_user").show()

    //创建一个新的SparkSession，可以通过已有的SparkSession创建
    val newSparkSession = spark.newSession()
    //多个SparkSession可以共享全局表
    newSparkSession.sql("select * from global_temp.t_user").show()

    dataFrame.createTempView("tt_user")
    spark.sql("select * from tt_user").show()
    spark.stop()
  }
}
