package cn.myfreecloud.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/6/29 17:05
 * @description:
 */
public class CountInterceptor implements ProducerInterceptor<String, String> {

    private int successCount = 0;
    private int errorCount = 0;


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            successCount++;
        } else {
            errorCount++;
        }
    }

    /**
     * 结束的时候调用一次的方法
     */
    @Override
    public void close() {
        System.out.println("发送成功"+successCount+":条数据!!!!!!!");
        System.out.println("发送失败"+errorCount+":条数据>>>>>>>>>");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
