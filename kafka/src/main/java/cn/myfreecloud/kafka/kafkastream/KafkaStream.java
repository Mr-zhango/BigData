package cn.myfreecloud.kafka.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @author: zhangyang
 * @date: 2019/6/29 17:24
 * @description:
 */
public class KafkaStream {
    public static void main(String[] args) {

        //创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();

        //创建配置文件
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "hadoop102:9092");
        properties.put("application.id","kafkaStream");

        //写入的数据
        builder.addSource("SOURCE","first").addProcessor("PROCESSOR", new ProcessorSupplier() {
            @Override
            public Processor get() {
                return new LoggerProcessor(){

                };
            }
        },"SOURCE").addSink("SINK","second","PROCESSOR");


        //写出到second这个topic
        KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);

        kafkaStreams.start();

    }
}
