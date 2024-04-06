package com.qr.daemon.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 生产者分区策略
 *  1、没有指明partition但是有key的情况下的消费者分区分配
 *  2、添加自定义分区器
 */
public class $_05CustomProducerCallbackKey {
    public static void main(String[] args) {
        //1、 创建kafka生产者对象配置
        Properties properties = new Properties();
        //2、 添加配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.qr.kafka.partitioner.MyPartitioner");
        // key、value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //3、 创建生产者
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        //4、 创造数据
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first", i + "", "kafka: " + i);
            producer.send(producerRecord,((metadata, e) -> {
                if (e == null) {
                    System.out.println("消息："+producerRecord.value()+", 主题：" + metadata.topic() + "->" + "分区：" + metadata.partition());
                }else {
                    e.printStackTrace();
                }
            }));
        }
        //5、 关闭连接
        producer.close();
    }
}
