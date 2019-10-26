package cn.myfreecloud.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author:zhangyang
 * @date:2019/6/29 10:44
 * @description:
 */
public class NewProducer {

    public static void main(String[] args) {


        Properties props = new Properties();

        //kafka集群,监听的端口号是9092
        props.put("bootstrap.servers", "hadoop102:9092");

        //ack的应答级别
        props.put("acks", "all");

        //重试次数
        props.put("retries", 0);

        //批量的大小
        props.put("batch.size", 16384);

        //提交延时
        props.put("linger.ms", 1);

        //缓存大小
        props.put("buffer.memory", 33554432);

        //提供序列化的类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //设置自定义的分区类
//        props.put("partitioner.class", ConsumerPartitioner.class);

        ArrayList<String> arrayList = new ArrayList<>();
        arrayList.add("cn.myfreecloud.kafka.interceptor.TimeInterceptor");
        arrayList.add("cn.myfreecloud.kafka.interceptor.CountInterceptor");

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,arrayList);

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //循环发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("second", String.valueOf(i)), new Callback() {

                //完成之后的回调函数
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){

                        String topic = metadata.topic();
                        System.out.println(topic+"------"+metadata.partition()+"-----------"+metadata.offset());

                        System.out.println("发送成功");
                    }else{
                        System.out.println("发送失败");
                    }
                }
            });
            
        }

        //关闭资源
        producer.close();
    }
}
