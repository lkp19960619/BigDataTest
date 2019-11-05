package streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Kafka DStream对Kafka生产者产生的数据进行实时流处理
  */
object KafkaDataSourceTest {
  def main(args: Array[String]): Unit = {
    //StreamingContext是所有Spark Streaming应用的入口
    val conf = new SparkConf().setAppName("kafka stream application").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //准备Kafka消费者对象
    val kafkaParams = Map[String, Object](
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092"),
      (ConsumerConfig.GROUP_ID_CONFIG, "g1"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,classOf[StringDeserializer])
    )
    //通过Kafka创建DStream对象
    val stream = KafkaUtils
      .createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](List("spark"), kafkaParams))//kafkaParams是从kafka拉取数据的消费者配置对象
    stream.map(record=>(record.key(),record.value())).print
    //操作:在操作的时候必须把record转换为scala的类型
    //启动计算
    ssc.start()
    //等待计算终止
    ssc.awaitTermination()
  }
}
