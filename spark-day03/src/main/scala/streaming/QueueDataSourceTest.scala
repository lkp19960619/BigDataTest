package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * RDD Queue测试
  * 通过两个RDD来创建队列流queueStream
  *
  * 将Spark RDD封装到Scala Queue队列集合中，通过Queue构建DStream，每一个微批只处理一个数据文件
  */
object QueueDataSourceTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("queue application")

    val ssc = new StreamingContext(conf,Seconds(5))
    val sc = ssc.sparkContext
    //设置控制台日志输出级别
    sc.setLogLevel("WARN")
    //通过rdd queue创建DStream对象
    val rdd1 = sc.textFile("file:///d:\\testdata\\1.txt")
    val rdd2 = sc.textFile("file:///d:\\testdata\\2.txt")
    val rdd3 = sc.textFile("file:///d:\\testdata\\3.txt")
    //把rdd放到队列中
    val rddQueue = mutable.Queue(rdd1,rdd2,rdd3)
    //创建队列流
    val stream = ssc.queueStream(rddQueue)
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
