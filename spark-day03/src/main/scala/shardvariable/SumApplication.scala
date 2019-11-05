package shardvariable

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数组累加之和为0的原因是因为无论是转换算子还是行动算子都是运行在计算节点中
  * 而外部变量是定义在Driver中
  *
  * 在Spark中，如果一个算子函数使用了外部的一个普通变量，这个变量会进行序列化并拷贝到任务节点
  * 每一个任务使用自己拷贝的变量副本
  */
object SumApplication {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("shard variable")
    val sc = new SparkContext(conf)

    //需求：累积RDD中的这些数据的和
    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5), 3)

    //    var count = 0
    //    //使用广播变量
    //    val broadcastVar = sc.broadcast(count)
    //
    //    rdd.foreach(n =>{ n + broadcastVar.value})
    //声明一个名为Counters的累加器
    val counters = sc.longAccumulator(name = "Counters")
    rdd.foreach(n=>{
      //将遍历产生的元素累加，累加器的累加结果可以返回给Driver
      counters.add(n)
    })

    println("数组累积之和为："+counters.value)

    sc.stop()
  }
}
