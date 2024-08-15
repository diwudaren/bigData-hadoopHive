package com.ar.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.ar.entity.TableProcess;
import com.ar.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // TODO 2.状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS),Time.of(3L,TimeUnit.HOURS)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","bigadmin");

        // TODO 3. 读取业务主流
        String topic = "topic_db";
        String groupId = "dim_sink_app";
        DataStreamSource<String> gmallDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 4.主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = gmallDS.map(JSON::parseObject);

        // TODO 5.主流 ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(jsonObj -> {
            try {
                jsonObj.getJSONObject("data");
                if (jsonObj.getString("type").equals("bootstrap-start") || jsonObj.getString("type").equals("bootstrap-complete")) {
                    return false;
                }
                return true;
            } catch (JSONException jsonException) {
                return false;
            }
        });
//        env.execute();

        // TODO 6. FlinkCDC 读取配置流并广播流
        // 6.1 FlinkCDC 读取配置表信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username("maxwell")
                .password("@DMMQrCyO0o123")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        // 6.2 封装为流
        DataStreamSource<String> mysqlDSSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");
        // 6.3 广播配置流
        MapStateDescriptor<String, TableProcess> tableConfigDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDSSource.broadcast(tableConfigDescriptor);

        // TODO 7. 连接流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastDS);

    }
}
