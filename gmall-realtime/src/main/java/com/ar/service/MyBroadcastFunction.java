package com.ar.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.ar.entity.TableProcess;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    private MapStateDescriptor<String, TableProcess> tableConfigDescriptor;

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> tableConfigDescriptor){
        this.tableConfigDescriptor = tableConfigDescriptor;
    }

    // 定义 Druid 连接池对象
    DruidDataSource druidDataSource;

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

    }

    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

    }
}
