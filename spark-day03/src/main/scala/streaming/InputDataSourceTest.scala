package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实时计算（单词计数）
  */
object InputDataSourceTest {
  def main(args: Array[String]): Unit = {
    //1.创建StreamingContext程序入口
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    //2.将DStream按照5秒时间间隔划分一个个的micro batch数据(RDD序列)
    val ssc = new StreamingContext(conf,Seconds(5))
    //关闭Streaming默认的INFO级别的日志
    ssc.sparkContext.setLogLevel("ERROR")
    //3.创建DStream
    val stream = ssc.textFileStream("hdfs://Spark01:9000/data")
    //4.对DStream应用高级算子
    stream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    //5.启动流数据处理的应用
    ssc.start()
    //6.优雅的关闭Streaming应用
    ssc.awaitTermination()
  }
}
