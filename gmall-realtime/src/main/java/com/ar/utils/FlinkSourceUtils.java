package com.ar.utils;

import com.alibaba.fastjson.JSONObject;
import com.ar.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkSourceUtils {
    public static SourceFunction<String> getKafkaSource(String groupId, String topic) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKERS);
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
    }

    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties proper = new Properties();
        proper.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,Constant.KAFKA_BROKERS);
        proper.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15 * 60 * 1000 +"");
        return new FlinkKafkaProducer<>(
                "default",
                (KafkaSerializationSchema<String>) (element, aLong) -> new ProducerRecord<>(topic, element.getBytes()),
                proper,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
