package com.baizhi.flink.evictor;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * 自定义剔除器，剔除包含指定字符串的数据
 * @param <W>   窗口类型
 */
public class ErrorEvictor<W extends Window> implements Evictor<String,W> {
    //输入数据内容
    private String content;
    //是否方法执行之后
    private boolean isEvictorAfter;

    public ErrorEvictor(String content) {
        this.content = content;
        this.isEvictorAfter = false;
    }

    public ErrorEvictor(String content, boolean isEvictorAfter) {
        this.content = content;
        this.isEvictorAfter = isEvictorAfter;
    }

    public void evictBefore(Iterable<TimestampedValue<String>> elements, int size, W window, EvictorContext evictorContext) {
        if(!isEvictorAfter){
            evict(elements,size,evictorContext);
        }
    }

    public void evictAfter(Iterable<TimestampedValue<String>> elements, int size, W window, EvictorContext evictorContext) {
        if(isEvictorAfter){
            evict(elements,size,evictorContext);
        }
    }

    private void evict(Iterable<TimestampedValue<String>> elements, int size, EvictorContext evictorContext){
        Iterator<TimestampedValue<String>> iterator = elements.iterator();
        while(iterator.hasNext()){
            TimestampedValue<String> next = iterator.next();
            String value = next.getValue();
            if(value.contains(content)){
                iterator.remove();
            }
        }
    }
}
