package com.qr.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * 自定义flume拦截器
 *
 */

public class CustomInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    /**
     * 对单个事件进行拦截
     * @param event 事件
     * @return 结果
     */
    @Override
    public Event intercept(Event event) {
        // 1、从事件中获取数据
        byte[] body = event.getBody();
        if (body[0] >= 'a' && body[0] <= 'z'){
            // 是字母就在事件头部设置type类型为letter
            event.getHeaders().put("type","letter");
        } else if (body[0] >= '0' && body[0] <= '9') {
            // 是数字就在事件头部设置type类型为number
            event.getHeaders().put("type","number");
        }
        // 返回事件
        return event;
    }

    /**
     * 对批量事件进行拦截
     * @param events 事件集
     * @return 结果
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        events.forEach(this::intercept);
        return events;
    }

    @Override
    public void close() {

    }

    // 拦截器对象的构造对象
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
