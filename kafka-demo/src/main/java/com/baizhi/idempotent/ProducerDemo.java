package com.baizhi.idempotent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class ProducerDemo {
    public static void main(String[] args) {
        //准备kafka生产者的配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092");
        //把record的key和value进行序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        //使用Kafka的幂等性
        //开启幂等操作支持
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        //设置应答策略
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        //设置重复次数
        properties.put(ProducerConfig.RETRIES_CONFIG,5);
        //设置请求超时时间
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,2000);

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //生产记录并发布
        for (int i = 20; i < 40; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("t3", UUID.randomUUID().toString(), "Hello,Kafka"+i);
            producer.send(record);

        }
        //释放资源
        producer.flush();
        producer.close();
    }
}
