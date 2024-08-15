package com.ar.service;

import com.ar.utils.FlinkSourceUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseAppV1 {

    protected abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);
    public void init(int port, int p, String ckPathGroupIdJobName, String topic){
        Configuration conf = new Configuration();
        conf.setInteger("rest.post",port);
        // 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(p);
        // 2、创建后端
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/"+ckPathGroupIdJobName);
        env.getCheckpointConfig().setCheckpointTimeout(20 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 3、读取业务主流
        DataStreamSource<String> stream = env.addSource(FlinkSourceUtils.getKafkaSource(ckPathGroupIdJobName, topic));
        this.handle(env,stream);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
