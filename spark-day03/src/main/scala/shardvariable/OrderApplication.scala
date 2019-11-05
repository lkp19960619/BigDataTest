package shardvariable

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：计算用户花费总额
  */
object OrderApplication {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("order application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //用一个变量类接收用户的信息
    val userInfo = Map("001" -> "zs", "002" -> "ls", "003" -> "ww")


    //订单的RDD
    val orderItems = List(
      ("001", OrderItem("iphone11", 4999, 2)),
      ("002", OrderItem("Dior", 129, 1)),
      ("001", OrderItem("刮胡刀", 15, 1)),
      ("003", OrderItem("收音机", 85, 1))
    )
    val rdd = sc.makeRDD(orderItems)
    //    不适用广播变量
    //    rdd
    //      .map(t2 => (t2._1, t2._2.price * t2._2.num)) //拿到用户订单花费总额
    //      .reduceByKey(_ + _) //把一个用户的两个订单项相加
    //      .foreach(t2 => {
    //      println(t2._1 + "\t" + userInfo(t2._1) + "\t" + t2._2)
    //    })
    //使用广播变量
    val broadcastVar = sc.broadcast(userInfo)
    rdd
      .map(t2 => (t2._1, t2._2.price * t2._2.num))
      .reduceByKey(_ + _)
      .foreach(t2 => {
        println(t2._1 + "\t" + broadcastVar.value(t2._1) + "\t" + t2._2)
      })
  }
}

//样例类
case class OrderItem(var productName: String, var price: Double, var num: Int)
