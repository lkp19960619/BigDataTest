package com.baizhi.ctp;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class ConsumeTransformProduceDemo {
    public static void main(String[] args) {
        //初始化生产者和消费者的配置对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig());

        //消费者订阅主题
        consumer.subscribe(Arrays.asList("t4"));

        //初始化事务
        producer.initTransactions();

        while(true){
            //开启事务
            producer.beginTransaction();
            try {
                //拉取数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    //需要业务处理的内容
                    System.out.println(record.key()+"\t"+record.value());
                    //生产者发布消息
                    producer.send(new ProducerRecord<String,String>("t5",record.value()));
                    //将消费位置记录到map集合中
                    offsets.put(new TopicPartition("t4",record.partition()),new OffsetAndMetadata(record.offset()+1));
                }
                //维护消费位置，将事务内的消费位置信息提交到kafka中
                producer.sendOffsetsToTransaction(offsets,"g1");
                producer.commitTransaction();
            } catch (Exception e) {
                e.printStackTrace();
                producer.abortTransaction();
            }
        }
    }

    public static Properties producerConfig(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        //设置事务的唯一标识
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        //开启幂等
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        //设置重复次数
        properties.put(ProducerConfig.RETRIES_CONFIG,5);
        //设置请求超时时间
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,3000);
        //批处理
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,2000);
        return properties;
    }

    public static Properties consumerConfig(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"g1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        //修改消费者默认的事务隔离级别
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        return properties;
    }
}
