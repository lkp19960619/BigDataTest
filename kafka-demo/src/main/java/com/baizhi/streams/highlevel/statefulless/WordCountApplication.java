package com.baizhi.streams.highlevel.statefulless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.ArrayList;
import java.util.Properties;

public class WordCountApplication {
    public static void main(String[] args) {
        //指定流处理应用的配置文件
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "HadoopNode01:9092,HadoopNode02:9092,HadoopNode03:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-highlevel-application");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        //编织拓扑任务
        StreamsBuilder builder = new StreamsBuilder();
        //生产主题转化为流对象
        KStream<String, String> stream = builder.stream("t7");
        //========================================匿名内部类==========================================
        /*stream.flatMap(new KeyValueMapper<String, String, Iterable<? extends KeyValue<?, ?>>>() {
         *//**
         *
         * @param key   null
         * @param value 英文短语
         * @return
         *//*
            @Override
            public Iterable<? extends KeyValue<?, ?>> apply(String key, String value) {
                //对value进行空格拆分
                String[] words = value.toLowerCase().split(" ");
                //要返回集合类型的对象
                ArrayList<KeyValue<String, String>> list = new ArrayList<>();
                //遍历单词数组，把遍历出来的单词放到集合中
                for (String word : words) {
                    KeyValue<String, String> keyValue = new KeyValue<String, String>(key,word);
                    list.add(keyValue);
                }
                return list;
            }
        });*/
        //===========================================================================================

        //========================================Lambda表达式=======================================
        //kTable用来保存计算结果
        KTable<String, Long> kTable = stream
                /**
                 * 处理完之后变成
                 * null hello
                 * null world
                 */
                .flatMap((key, value) -> {
                    String[] words = value.toLowerCase().split(" ");
                    ArrayList<KeyValue<String, String>> list = new ArrayList<>();
                    for (String word : words) {
                        KeyValue<String, String> keyValue = new KeyValue<>(key, word);
                        list.add(keyValue);
                    }
                    return list;
                })
                /**
                 * 处理完之后变成(给予单词初始值)
                 * hello 1L
                 * world 1L
                 */
                .map((k, v) -> new KeyValue<>(v, 1L))    //map方法的作用是把一个对象映射成另一个对象
                /**
                 * 把单词相同的k，v归为一类
                 */
                // hello [1,1,...]
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                //对key相同的集合做一个累加
                .count();
        /**
         * toStream()把kTable转换成流
         * to()方法的两个参数分别指定输出主题和输出时的序列化器
          */
        kTable.toStream().to("t8", Produced.with(Serdes.String(),Serdes.Long()));
        //===========================================================================================
        //初始化流处理应用
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        //启动流处理应用
        kafkaStreams.start();
    }

}
