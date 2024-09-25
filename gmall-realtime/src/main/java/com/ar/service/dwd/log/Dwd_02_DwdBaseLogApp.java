package com.ar.service.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ar.common.Constant;
import com.ar.service.BaseAppV1;
import com.ctc.wstx.sr.CompactNsContext;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.util.parsing.json.JSONArray;

import java.time.Duration;

public class Dwd_02_DwdBaseLogApp extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_02_DwdBaseLogApp().init(
                3002,
                2,
                "Dwd_02_DwdBaseLogApp",
                Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((obj,ts)->obj.getLong("ts"))

                );
        //
    }
}
