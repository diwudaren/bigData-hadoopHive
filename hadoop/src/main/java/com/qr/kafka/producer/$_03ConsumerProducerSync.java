package com.qr.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 同步发送API
 *  同步发送消息的生产者
 */
public class $_03ConsumerProducerSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1、 创建kafka生产者配置对象
        Properties properties = new Properties();
        //2、给kafka添加配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        // key、value 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //3、创建生产者对象
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        //4、调用send方法发送数据
        for (int i = 0; i < 10; i++) {
            //添加回调
            producer.send(new ProducerRecord<>("first","kafka: " + i)).get();
        }
        //5、关闭连接
        producer.close();
    }
}
