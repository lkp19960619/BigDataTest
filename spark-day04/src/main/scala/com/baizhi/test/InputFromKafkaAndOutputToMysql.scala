package com.baizhi.test

import java.sql.{Driver, DriverManager}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从Kafka中读取数据，并存储到Mysql数据库中
  */
object InputFromKafkaAndOutputToMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input from kafka and output to mysql").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置日志级别
    ssc.sparkContext.setLogLevel("ERROR")
    //通过Kafka创建DStream对象
    //添加Kafka配置信息
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "g2",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val message = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("spark"), kafkaParams))

    message
      .flatMap(_.value.split(" "))
      .map((_, 1))
      .reduceByKey((v1: Int, v2: Int) => v1 + v2, 1)
      .foreachRDD(rdd => {
        //遍历每个分区
        rdd.foreachPartition(iter => {
          classOf[Driver]
          val connection = DriverManager.getConnection("jdbc:mysql://192.168.170.1:3306/test", "root", "960619")
          val selectSql = "select * from t_word where word = ?"
          val updateSql = "update t_word set count = ? where word = ?"
          val insertSql = "insert into t_word values(?,1)"
          iter.foreach(record => {//record是一个二元组->(word,count)
            val queryStatement = connection.prepareStatement(selectSql)
            queryStatement.setString(1, record._1)
            val rs = queryStatement.executeQuery()
            //word存在
            if (rs.next()) {
              val count = rs.getInt("count")
              val updateStatement = connection.prepareStatement(updateSql)
              updateStatement.setInt(1, count + record._2)
              updateStatement.setString(2,record._1)
              updateStatement.executeUpdate()
            }else{
              val insertStatement = connection.prepareStatement(insertSql)
              insertStatement.setString(1,record._1)
              insertStatement.executeUpdate()
            }
          })
          connection.close()
        })
      })

    //启动任务
    ssc.start()
    //优雅的关闭
    ssc.awaitTermination()

  }
}
