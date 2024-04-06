package com.qr.daemon.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 *
 */
public class MyPartitioner implements Partitioner {

    /**
     * 分区方法
     * @param topic topic
     * @param key key
     * @param keyBytes key字节码
     * @param value value
     * @param valueBytes value字节码
     * @param cluster 分区个数
     * @return 分区号
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        //1、 获取key
        String keySrt = key.toString();
        //2、 创建分区返回结果
        int parNum;
        //3、 计算key的hash值
        int keyStrHash = keySrt.hashCode();
        //4、 获取topic个数
        Integer partitionNumber = cluster.partitionCountForTopic(topic);
        //5、 计算分区号
        parNum = Math.abs(keyStrHash) % partitionNumber;
        //6、 返回分区号
        return parNum;
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {

    }

    /**
     * 配置方法
     * @param map map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
