package com.baizhi.customserialization;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * 自定义对象类型序列化和反序列化器
 *
 * 需要实现Kafka提供的序列化接口和反序列化接口
 */
public class ObjectCode implements Serializer, Deserializer {

    /**
     * 依赖commons-lang包提供的工具类
     *
     * byte[] ----> Object
     *
     * @param s
     * @param bytes
     * @return
     */
    @Override
    public Object deserialize(String s, byte[] bytes) {
        return SerializationUtils.deserialize(bytes);
    }

    @Override
    public void configure(Map map, boolean b) {

    }


    /**
     * Object -----> byte[]
     * @param s
     * @param o
     * @return
     */
    @Override
    public byte[] serialize(String s, Object o) {
        return SerializationUtils.serialize((Serializable) o);
    }

    @Override
    public void close() {

    }
}
