package cn.myfreecloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author: zhangyang
 * @date: 2019/6/29 12:16
 * @description: 一个消费者可以消费多个生产者的数据
 */
public class CustomerConsumer {
    public static void main(String[] args) {

        //所有的配置信息
        Properties props = new Properties();

        //kafka集群
        props.put("bootstrap.servers", "hadoop102:9092");
        //消费者组id
        props.put("group.id", "test");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //是否自动提交offset
        props.put("enable.auto.commit", "true");

        //自动提交的延时
        props.put("auto.commit.interval.ms", "1000");


        //KV的反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //指定topic
        consumer.subscribe(Arrays.asList("second","first","third"));

        while (true){

            //获取数据
            ConsumerRecords<String, String> poll = consumer.poll(100);

            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(
                        record.topic()+"------"+record.partition()+"------"+record.value()
                );
            }

        }


    }
}
