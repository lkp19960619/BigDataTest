package com.baizhi.flink.datasink


import java.util.Properties

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{ByteArraySerializer, BytesSerializer, StringSerializer}

/**
  * 把计算结果输出到Kafka中
  */
object FlinkWordCountKafkaSink {
  def main(args: Array[String]): Unit = {
    //创建StreamExecutionEnvironment
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(3)
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"Spark01:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[ByteArraySerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[ByteArraySerializer])
    //重试次数
    prop.put(ProducerConfig.RETRIES_CONFIG,"3")
    //开启ACK应答机制
    prop.put(ProducerConfig.ACKS_CONFIG,"-1")
    //解决Kafka的幂等性
    prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")
    //设置批处理的大小
    prop.put(ProducerConfig.BATCH_SIZE_CONFIG,"100")
    //设置多长时间为一批
    prop.put(ProducerConfig.LINGER_MS_CONFIG,"500")
    //创建DataStream
    val dataStream: DataStream[String] = environment.socketTextStream("Spark01",9999)
    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .addSink(new FlinkKafkaProducer[(String, Int)]("topic01",new UserDefineKafkaSink,prop))
    environment.execute()
  }
}
