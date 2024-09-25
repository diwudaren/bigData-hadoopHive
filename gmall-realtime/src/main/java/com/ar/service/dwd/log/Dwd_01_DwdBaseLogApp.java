package com.ar.service.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.ar.common.Constant;
import com.ar.service.BaseAppV1;
import com.ar.utils.FlinkSourceUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class Dwd_01_DwdBaseLogApp extends BaseAppV1 {

    private final String START ="start";
    private final String DISPLAY ="display";
    private final String ACTION ="action";
    private final String PAGE ="page";
    private final String ERR ="err";

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","bigadmin");
        new Dwd_01_DwdBaseLogApp().init(3001, 2, "DwdBaseLogApp", Constant.TOPIC_ODS_LOG);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1、 etl
        SingleOutputStreamOperator<JSONObject> etlEdStream = etl(stream);
        // 2、 纠正新老客户标签
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOid(etlEdStream);
        // 3、 分流
        Map<String, DataStream<JSONObject>> streams = splitStream(validatedStream);
        // 4、 写出到kafka中
        writeToKafka(streams);

    }

    private void writeToKafka(Map<String, DataStream<JSONObject>> streams) {
        streams.get(START).map(JSONAware::toJSONString).addSink(FlinkSourceUtils.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        streams.get(DISPLAY).map(JSONAware::toJSONString).addSink(FlinkSourceUtils.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streams.get(ACTION).map(JSONAware::toJSONString).addSink(FlinkSourceUtils.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        streams.get(PAGE).map(JSONAware::toJSONString).addSink(FlinkSourceUtils.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streams.get(ERR).map(JSONAware::toJSONString).addSink(FlinkSourceUtils.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
    }

    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream) {
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display"){};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action"){};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page"){};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err"){};

        // 5个流：
        // 启动日志：主流
        // 页面 曝光 活动 错误：测输出流
        SingleOutputStreamOperator<JSONObject> startStream = validatedStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject obj,
                                       Context ctx,
                                       Collector<JSONObject> out) throws Exception {
                if (obj.containsKey("start")) {
                    // 启动日志
                    out.collect(obj);
                } else {
                    JSONObject common = obj.getJSONObject("common");
                    JSONObject page = obj.getJSONObject("page");
                    Long ts = obj.getLong("ts");

                    //1、 曝光
                    JSONArray displays = obj.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.putAll(common);
                            display.putAll(page);
                            display.put("ts", ts);
                            ctx.output(displayTag, display);
                        }
                        obj.remove("displays");
                    }
                    //2、 活动
                    JSONArray actions = obj.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.putAll(common);
                            action.putAll(page);
                            ctx.output(actionTag, action);

                        }
                        obj.remove("actions");
                    }
                    // 3、错误
                    if (obj.containsKey("err")) {
                        ctx.output(errTag, obj);
                        obj.remove("err");
                    }
                    if (page != null) {
                        ctx.output(pageTag, obj);
                    }

                }
            }
        });
        Map<String, DataStream<JSONObject>> result = new HashMap<>();
        result.put(START, startStream);
        result.put(DISPLAY, startStream.getSideOutput(displayTag));
        result.put(ACTION, startStream.getSideOutput(actionTag));
        result.put(PAGE, startStream.getSideOutput(pageTag));
        result.put(ERR, startStream.getSideOutput(errTag));
        return result;
    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOid(SingleOutputStreamOperator<JSONObject> etlEdStream) {
        return etlEdStream
                .keyBy(obj->obj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> firstVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                JSONObject common = value.getJSONObject("common");
                Long ts = value.getLong("ts");
                String today = format.format(ts);
                String firstVisit = firstVisitDateState.value();
                if ("1".equals(common.getString("is_new"))){
                    if (firstVisit ==null){
                        firstVisitDateState.update(today);
                    }else if (!today.equals(firstVisit)){
                        common.put("is_new","0");
                    }
                }else {
                    String yesterday = format.format(ts - (24 * 60 * 60 * 1000));
                    firstVisitDateState.update(yesterday);
                }
                return value;
            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(json -> {
            try {
                JSON.parseObject(json);
            } catch (Exception e) {
                System.out.println("json格式有误，请检查：" + json);
                return false;
            }
            return true;
        }).map(JSON::parseObject);
    }
}
