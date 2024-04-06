package com.qr.project.offLine.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimestampAndTableNameInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(log);
        Long ts = jsonObject.getLong("ts");
        String timeMails = String.valueOf(ts * 1000);
        String table = jsonObject.getString("table");

        headers.put("timestamp",timeMails);
        headers.put("tableName",table);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        list.forEach(this::intercept);
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimestampAndTableNameInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
