package com.baizhi.customserialization;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        //准备kafka消费者的配置信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092");
        //设置反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ObjectCode.class);
        //设置消费组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");

        //设置消费者的消费策略,有earliest和lastest两种
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //关闭消费位置offset的自动提交功能
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        //创建消费者对象
        KafkaConsumer<String, User> consumer = new KafkaConsumer<String, User>(properties);

        //订阅主题topic
        consumer.subscribe(Arrays.asList("t3"));

        //拉取产生的新纪录
        while(true){
            //设置拉取超时时间
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, User> record : records) {
                User user = record.value();
                System.out.println(user);
            }
            //手动提交消费位置
            consumer.commitAsync();
        }


    }
}
