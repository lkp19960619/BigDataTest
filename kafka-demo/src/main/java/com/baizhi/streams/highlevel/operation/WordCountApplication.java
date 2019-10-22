package com.baizhi.streams.highlevel.operation;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

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
        KStream<String, String> stream = builder.stream("t7");
        //===============================branch操作算子===========================================
        /*KStream<String, String>[] kStreams = stream.branch(
                //输出以A开头的，过滤掉不以A开头的
                (k,v) -> v.startsWith("A"),
                (k,v) ->true
        );
        kStreams[0].foreach((k,v) -> System.out.println(k+"\t"+v));*/
        //=======================================================================================
        //===============================filter操作算子===========================================
        /**
         * 类型   KStream->KStream
         * 作用   保留符合条件的数据
         */
//        stream
//                .filter((k, v) -> v.startsWith("H"))
//                .foreach((k,v) -> System.out.println(k+"\t"+v));
        //========================================================================================
        //===============================filterNot操作算子===========================================
        /**
         * 类型   KStream->KStream    KTable->KTable
         * 作用   保留不符合条件的数据
         */
//        stream
//                .filterNot((k,v) -> v.startsWith("K"))
//                .foreach((k,v) -> System.out.println(k+"\t"+v));
        //========================================================================================
        //===============================FlatMap操作算子===========================================
        /**
         * 类型   KStream->KStream
         * 操作   将一个Record展开为0-n个Record
         */
//        stream
//                .flatMap((k,v) -> Arrays.asList(
//                        new KeyValue<String,String>(k,v.toUpperCase()),
//                        new KeyValue<String,String>(k,v.toLowerCase())))
//                .foreach((k,v) -> System.out.println(k+"\t"+v));
        //========================================================================================
        //===============================flatMapValues操作算子====================================
        /**
         * 类型   KStream->KStream
         * 操作   讲一个Record的value展开为1到N个新的value（key不变）
         * 例如
         * null Hello World --> null Hello 和 null World
         */
//        stream
//                .flatMapValues((v) -> Arrays.asList(v.split(" ")))//对值进行按照空格展开
//                .foreach((k,v) -> System.out.println(k+"\t"+v));
        //========================================================================================
        //===============================GroupBy操作算子=======================================
        /**
         * 类型   KStream -> KGroupedStream
         * 作用   根据key或者value进行分组,分组的时候会进行Shuffle（洗牌）
         */
//        stream
//                .flatMapValues((v) -> Arrays.asList(v.split(" ")))
//                //根据v进行分组
//                .groupBy((k,v) -> v)
//                .count()
//                .toStream()
//                .foreach((k,v) -> System.out.println(k+"\t"+v));
        //========================================================================================
        //===============================GroupByKey操作算子=======================================
        /**
         * 类型   KStream->KGroupedStream
         * 作用   根据已存在的key的值进行分区操作(洗牌)
         */
        /*stream
                // 转换之后是null Hello类型
                .flatMapValues((v) -> Arrays.asList(v.split(" ")))
                //重新映射
                .map((k,v) -> new KeyValue<String,Long>(v,1L))
                //做分组处理，相当于洗牌操作
                .groupByKey(Grouped.with(Serdes.String(),Serdes.Long()))
                //做聚合处理
                .count()
                //转换为流
                .toStream()
                //做输出
                .foreach((k,v) -> System.out.println(k+"\t"+v));*/
        //========================================================================================
        //===============================mapValues操作算子========================================
        /**
         * 类型   KStream->KStream
         * 操作   类似于map操作，不同的是key不可变，v可变
         */
        /*stream
                //null Hello
                .flatMapValues((v) -> Arrays.asList(v.split(" ")))
                .map((k,v)->new KeyValue<String,Long>(v,1L))
                .mapValues(v -> v+1)
                .foreach((k,v)-> System.out.println(k+"\t"+v));*/
        //========================================================================================
        //===============================Merge操作算子========================================
        /**
         * 类型   KStream->KStream
         * 操作   将两个流合并为一个大流
         */
        /*//分流，得到的是一个KStream类型的数组
        KStream<String, String>[] streams = stream.branch(
                (k, v) -> v.startsWith("A"),
                (k, v) -> v.startsWith("B"),
                (k, v) -> true
        );
        //把数组中的第0号和第1号合并
        streams[0].merge(streams[1])
                .foreach((k,v) -> System.out.println(k+"\t"+v));*/
        //========================================================================================
        //===============================Peek操作算子=============================================
        /**
         * 类型   KStream->KStream
         * 操作   探针(调试程序)，不会改变数据流内容
         */
       /* stream.peek((k, v) -> System.out.println(k + "\t" + v))
                .print(Printed.toSysOut());*/
        //========================================================================================
        //===============================SelectKey操作算子========================================
        /**
         * 类型   KStream->KStream
         * 操作   给流中的数据，分配新的k值（k变，v不变），可以指定k的值
         */
        stream
                .selectKey((k,v)->"String")
                .print(Printed.toSysOut());
        //========================================================================================
        Topology topology = builder.build();
        //打印自动生产的Topology信息
        System.out.println(topology.describe().toString());
        //初始化流处理应用
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        //启动流处理应用
        kafkaStreams.start();
    }
}
