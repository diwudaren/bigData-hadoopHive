package com.ar.service.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ar.common.Constant;
import com.ar.service.BaseAppV1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DwdBaseLogApp extends BaseAppV1 {

    public static void main(String[] args) {
        new DwdBaseLogApp().init(3001,2,"DwdBaseLogApp", Constant.TOPIC_ODS_LOG);
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1、 etl
        SingleOutputStreamOperator<JSONObject> etlEdStream = etl(stream);
        // 2、 纠正新老客户标签

        // 3、 分流

        // 4、 写出到kafka中

    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(json->{
            try {
                JSON.parseObject(json);
            } catch (Exception e) {
                System.out.println("json格式有误，请检查："+json);
                return false;
            }
            return true;
        }).map(JSON::parseObject);
    }
}
