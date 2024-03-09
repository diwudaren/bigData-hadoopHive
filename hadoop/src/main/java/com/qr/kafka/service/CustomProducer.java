package com.qr.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
/**
 * @author: jtz
 * @date: 2024/3/4 14:16
 * data kafka生产者
 **/
public class CustomProducer {
    public static void main(String[] args) {
        // 创建生产者对象
        Properties properties = new Properties();
        // 创建连接
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"11.0.24.223:9092");

        // key,value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer  = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            //调用send方法发送数据
            System.err.println(i);
            kafkaProducer .send(new ProducerRecord<>("first","kafka" + i));
        }

        // 关闭资源
        kafkaProducer.close();
        System.err.println("========================");
    }
}
