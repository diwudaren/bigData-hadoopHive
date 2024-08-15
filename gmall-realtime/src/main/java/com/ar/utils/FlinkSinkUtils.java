package com.ar.utils;

import com.alibaba.fastjson.JSONObject;
import com.ar.entity.TableProcess;
import com.ar.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkSinkUtils {
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {

        return new PhoenixSink();
    }
}
