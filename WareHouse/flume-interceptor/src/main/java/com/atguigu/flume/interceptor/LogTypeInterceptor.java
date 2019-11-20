package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: zhangyang
 * @description: 自定义一个分类拦截器
 * @Param: 
 * @return: 
 * @time: 2019/11/19 16:00
 */    
public class LogTypeInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    /**
     * @Author: zhangyang
     * @description: 单个event处理
     * @Param:
     * @return:
     * @time: 2019/11/19 16:01
     */
    @Override
    public Event intercept(Event event) {

        // 区分日志类型：   body  header

        // 1 获取body数据
        byte[] body = event.getBody();

        String log = new String(body, Charset.forName("UTF-8"));

        // 2 获取header
        Map<String, String> headers = event.getHeaders();

        // 3 判断数据类型并向Header中赋值
        if (log.contains("start")) {
            headers.put("topic","topic_start");
        }else {
            headers.put("topic","topic_event");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event intercept1 = intercept(event);

            interceptors.add(intercept1);
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements  Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
