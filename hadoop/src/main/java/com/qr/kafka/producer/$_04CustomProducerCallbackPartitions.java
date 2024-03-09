package com.qr.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者分区策略
 *  将数据发送到指定partition的情况下，如：将所有消息发送到分区1中。
 */
public class $_04CustomProducerCallbackPartitions {
    public static void main(String[] args) {
        //1、创建kafka生产者对象配置
        Properties properties = new Properties();
        //2、 添加配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //key、value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //3、创建生产者
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        //4、发送消息、
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first",1,"","kafka: "+i),((recordMetadata, e) -> {
                if (e == null){
                    System.out.println("主题：" + recordMetadata.topic() + "->"  + "分区：" + recordMetadata.partition());
                }else {
                    e.printStackTrace();
                }
            }));
        }
        //5、关闭连接
        producer.close();
    }
}
