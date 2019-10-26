package cn.myfreecloud.kafka;

import cn.myfreecloud.kafka.kafkastream.LoggerProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @author: zhangyang
 * @date: 2019/6/29 10:44
 * @description:
 */
public class MainApplication {
    public static void main(String[] args) {
        // 设置配置信息
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"filter");
        // brocker的端口号
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

//        StreamsConfig streamsConfig = new StreamsConfig(props);

        // 创建拓扑结构
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // 1.设置输入端
        topologyBuilder.addSource("source","first")

        // 2.设置处理端
        // processor:处理数据阶段的名称; ProcessorSupplier:具体的数据处理的方法; source:本次模块的输入源是谁?
        .addProcessor("processor", new ProcessorSupplier<byte[],byte[]>() {
            @Override
            public Processor<byte[],byte[]> get() {
                // 具体的processor处理类
                return new LoggerProcessor();
            }
        },"source")

        // 3.设置输出端
        // sink:跟flume相似,表明这是sink端,second:表示输出到那个topic,
        .addSink("sink","second","processor");

        // 输出流程
        KafkaStreams kafkaStreams = new KafkaStreams(topologyBuilder, props);

        // 启动,开始工作
        kafkaStreams.start();

    }
}
