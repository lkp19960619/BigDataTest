package com.baizhi.customserialization;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class ProducerDemo {
    public static void main(String[] args) {
        //准备kafka生产者的配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092");
        //把record的key和value进行序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ObjectCode.class);


        //创建生产者对象
        KafkaProducer<String, User> producer = new KafkaProducer<String, User>(properties);

        //生产记录并发布
        for (int i = 0; i < 20; i++) {
            //key不为null，第一种策略
            ProducerRecord<String, User> record = new ProducerRecord<String, User>("t3", UUID.randomUUID().toString(),new User(i,"zhangsan"+i,new Date()));

            //key为null，轮询策略
            producer.send(record);

        }
        //释放资源
        producer.flush();
        producer.close();
    }
}
