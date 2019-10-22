package com.baizhi.streams.lowlevel.stateful.remote;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class WordCountProcessor implements Processor<String,String> {

    private ProcessorContext processorContext;
    private KeyValueStore<String,Long> state;

    /**
     *
     * @param processorContext  处理器的上下文对象
     */
    @Override
    public void init(ProcessorContext processorContext) {
        //从本地状态缓存器中拿累积状态对象
        this.state = (KeyValueStore<String, Long>) processorContext.getStateStore("Counts");

        this.processorContext = processorContext;
        //周期性将处理器的处理结果发送给下游的处理器
        //第三个参数代表周期性要进行的操作
        processorContext.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, new Punctuator() {
            //指定周期性要执行的方法
            @Override
            public void punctuate(long l) {
                KeyValueIterator<String, Long> iterator = state.all();
                while(iterator.hasNext()){
                    //获取到一个KeyValue对象
                    KeyValue<String, Long> keyValue = iterator.next();
                    //将当前处理器的结果转发给下游处理器
                    processorContext.forward(keyValue.key,keyValue.value);
                }
                iterator.close();
            }
        });
        processorContext.commit();
    }

    @Override
    public void process(String key, String value) {
        String[] words = value.split(" ");
        for (String word : words) {

            //如果key存在，value的值为word的值，如果不存在，赋值默认值
            Long num = state.get(word);
            System.out.println(word+"\t"+num);
            if(num == null){
                state.put(word,1L);
            }else{
                state.put(word,num+1L);
            }
        }
    }

    /**
     * 统计的结果需要发送给下游的sink组件，发送到外部的主题当中，进行结果的存储
     */

    @Override
    public void close() {

    }
}
