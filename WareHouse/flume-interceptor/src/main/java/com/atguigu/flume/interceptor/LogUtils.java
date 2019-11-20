package com.atguigu.flume.interceptor;
import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {

    /**
     * @Author: zhangyang
     * @description: 校验事件日志
     * @Param:
     * @return:
     * @time: 2019/11/19 15:52
     */
    public static boolean validateEvent(String log) {
        // 服务器时间 | json
        // 1549696569054 | {"cm":{"ln":"-89.2","sv":"V2.0.4","os":"8.2.0","g":"M67B4QYU@gmail.com","nw":"4G","l":"en","vc":"18","hw":"1080*1920","ar":"MX","uid":"u8678","t":"1549679122062","la":"-27.4","md":"sumsung-12","vn":"1.1.3","ba":"Sumsung","sr":"Y"},"ap":"weather","et":[]}

        // 1 切割 转义 | 的写法
        String[] logContents = log.split("\\|");

        // 2 校验
        if(logContents.length != 2){
            return false;
        }

        //3 校验服务器时间
        if (logContents[0].length()!=13 || !NumberUtils.isDigits(logContents[0])){
            return false;
        }

        // 4 校验json
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")){
            return false;
        }

        return true;
    }

    /**
     * @Author: zhangyang
     * @description: 校验启动日志
     * @Param:
     * @return:
     * @time: 2019/11/19 15:52
     */
    public static boolean validateStart(String log) {

        if (log == null){
            return false;
        }

        // 校验json
        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")){
            return false;
        }

        return true;
    }
}
