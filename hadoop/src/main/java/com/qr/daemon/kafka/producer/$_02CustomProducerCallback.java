package com.qr.daemon.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 异步发送API
 *  带回调函数的异步发送
 */
public class $_02CustomProducerCallback {
    public static void main(String[] args) {
        //1、 创建kafka生产者配置对象
        Properties properties = new Properties();
        //2、 给kafka添加配置文件
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        // key、value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        //3、 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //5、 调用send方法,发送消息
        for (int i = 0; i < 10; i++) {
            // 添加回调函数
            producer.send(new ProducerRecord<>("first","kafka: " + i), (recordMetadata, e) -> {
                if (e == null){
                    // 没有异常,输出信息到控制台
                    System.out.println("主题"+recordMetadata.topic() +", 分区："+recordMetadata.partition()+", 偏移量："+recordMetadata.offset());
                }
            });
        }
        //5、关闭连接
        producer.close();
    }
}
