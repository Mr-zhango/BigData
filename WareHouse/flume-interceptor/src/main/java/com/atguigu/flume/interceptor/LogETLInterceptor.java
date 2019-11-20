package com.atguigu.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @create: 2019-11-19 15:50
 * @author: Mr.Zhang
 * @description:
 **/

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        /**
         * @Author: zhangyang
         * @description: 进行ETL操作
         * @Param: [event]
         * @return: org.apache.flume.Event
         * @time: 2019/11/19 15:50
         */

        // 1 获取数据
        byte[] body = event.getBody();

        // 方式乱码
        String log = new String(body, Charset.forName("UTF-8"));


        // 2 判断数据类型并向Header中赋值
        if (log.contains("start")) {
            if (LogUtils.validateStart(log)) {
                return event;
            }
        } else {
            if (LogUtils.validateEvent(log)) {
                return event;
            }
        }

        // 3 返回校验结果
        return null;
    }


    @Override
    public List<Event> intercept(List<Event> events) {

        // 初始化list集合
        ArrayList<Event> interceptors = new ArrayList<>();

        for (Event event : events) {
            Event intercept1 = intercept(event);

            // 校验通过
            if (intercept1 != null) {
                interceptors.add(intercept1);
            }
        }

        return interceptors;
    }

    @Override
    public void close() {

    }

    /**
     * @Author: zhangyang
     * @description: 创建一个静态的内部类,主要是用于flume调用该方法,启动ETL过滤函数
     * @Param:
     * @return:
     * @time: 2019/11/19 15:58
     */
    public static class Builder implements Interceptor.Builder {

        /**
         * @Author: zhangyang
         * @description: 构建方法
         * @Param:
         * @return:
         * @time: 2019/11/19 15:59
         */
        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        /**
         * @Author: zhangyang
         * @description: 配置
         * @Param:
         * @return:
         * @time: 2019/11/19 15:59
         */
        @Override
        public void configure(Context context) {

        }
    }
}
