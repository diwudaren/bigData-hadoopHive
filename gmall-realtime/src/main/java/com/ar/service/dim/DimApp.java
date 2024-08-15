package com.ar.service.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ar.common.Constant;
import com.ar.entity.TableProcess;
import com.ar.service.BaseAppV1;
import com.ar.utils.FlinkSinkUtils;
import com.ar.utils.JdbcUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

public class DimApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DimApp().init(2001, 2, "DimApp", Constant.TOPIC_ODS_DB);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1、 ETL
        SingleOutputStreamOperator<JSONObject> etlStream = this.etl(stream);
        etlStream.print();
        // 2、 读取配置信息
        SingleOutputStreamOperator<TableProcess> tabStream = this.readTableProcess(env);
        // 3、 数据流和广播流做connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream = connect(etlStream, tabStream);
        // 4、过滤掉不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = filterNotNeedColumns(dataTpStream);
        // 5、根据不同的配置信息，把不同的维度写入到不同的phoenix的表中
        writeToPhoenix(resultStream);
    }

    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {

        // 自定义sink
        stream.addSink(FlinkSinkUtils.getPhoenixSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterNotNeedColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpStream) {
       return dataTpStream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {
                JSONObject data = value.f0;
                TableProcess tp = value.f1;
                List<String> columns = Arrays.asList(tp.getSinkColumns().split(","));
                // data其实是个map，从map中删除键值对
                data.keySet().removeIf(key-> !columns.contains(key) && !"op_type".equals(key));
                return value;
            }
        });

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<JSONObject> etlStream, SingleOutputStreamOperator<TableProcess> tabStream) {
        // 0、根据配置信息，在phoenix中创建相应的维度表
        tabStream = tabStream.map(new RichMapFunction<TableProcess, TableProcess>() {
            Connection conn;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = JdbcUtils.getPhoenixConnection();
            }

            @Override
            public TableProcess map(TableProcess tp) throws Exception {
                // 1、拼接sql语句
                StringBuilder sql = new StringBuilder();
                sql.append("create table if not exists ")
                        .append(tp.getSinkTable())
                        .append("(")
                        .append(tp.getSinkColumns().replaceAll("[^,]+", "$0 varchar"))
                        .append("constraint pk primary key (")
                        .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk())
                        .append("))")
                        .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
                System.out.println("phoenix建表语句：" + sql);
                // 2、获取预处理语句
                PreparedStatement ps = conn.prepareStatement(sql.toString());
                // 3、 给sql中的占位符赋值（增删改查），ddl：建表语句一般不会有占位符
                //略

                // 4、执行
                ps.execute();
                // 5、关闭
                ps.close();

                return tp;
            }

            @Override
            public void close() throws Exception {
                JdbcUtils.closeConnection(conn);
            }
        });

        // 1、把配置流设置成广播流，key: source_table, value: TableProcess
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tabStream.broadcast(tpStateDesc);
        // 2、让数据流去connect广播流
       return etlStream.connect(tpBcStream).process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public void processElement(JSONObject value,
                                       ReadOnlyContext ctx,
                                       Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                // 4、处理数据流中数据的时候，从广播流状态读取他对应的配置信息
                ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                // 根据MySQL中的表获取到配置信息
                String key = value.getString("table");
                TableProcess tp = state.get(key);
                // 如果不是维度表或者不需要sink的维度表，tp应该是null
                if(tp != null){
                    // 数据中的元数据信息无用了，可以只读取数据信息
                    JSONObject data = value.getJSONObject("data");
                    // 操作类型写入到data中，后面有用
                    data.put("op_type",value.getString("type"));
                    out.collect(Tuple2.of(data,tp));
                }

            }

            @Override
            public void processBroadcastElement(TableProcess value,
                                                Context ctx,
                                                Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                // 3、把配置流信息写入到广播状态
                String key = value.getSourceTable();
                BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                state.put(key, value);
            }
        });

    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username("maxwell")
                .password("@DMMQrCyO0o123")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    return obj.getObject("after", TableProcess.class);
                });
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {

        return stream.filter(json -> {
            try {
                JSONObject obj = JSON.parseObject(json.replace("bootstrap-", ""));
                return "gmall112022".equals(obj.getString("database"))
                        && ("insert".equals(obj.getString("type")) || "update".equals(obj.getString("type")))
                        && obj.getString("data") != null
                        && obj.getString("data").length() > 2;
            } catch (Exception e) {
                System.out.println("json 格式有误，你的数据是： " + json);
                return false;
            }
        }).map(JSON::parseObject);

    }
}
