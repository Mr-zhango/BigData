package com.atguigu.practice.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;


/**
 * Created by didi on 17/10/20.
 */
public class ProducerTest {
    private static final String TOPIC = "test"; //kafka创建的topic
    //private static final String CONTENT = "{\"business_id\":\"273\",\"user_id\":“11111111111”,\"order_id\":“222222”,\"trip_id\":\"0\",\"city_id\":\"17\",\"area_id\":\"567\",\"status\":\"0\",\"before_status\":\"0\",\"passenger_cnt\":\"1\",\"order_post_lat\":\"30.653605840019345\",\"order_post_lng\":\"104.10926820150458\",\"start_station_id\":\"77946\",\"end_station_id\":\"71030\",\"now_name\":\"成都成华城东医院\",\"start_lat\":\"30.65358\",\"start_lng\":\"104.10871\",\"transaction_id\":\"\",\"start_name\":\"肯德基(双林DT店)\",\"start_station_name\":\"双林社区-东北二门\",\"end_station_name\":\"仁和苑-北门\",\"start_station_lat\":\"30.653064\",\"start_station_lng\":\"104.109682\",\"end_station_lat\":\"30.655417\",\"end_station_lng\":\"104.115828\",\"end_lat\":\"30.65474\",\"end_lng\":\"104.11637\",\"end_name\":\"成都市沙河东篱幼儿园\",\"drive_time\":\"0\",\"estimated_stop_time_origin\":\"0\",\"estimated_stop_time\":\"0\",\"price\":\"499\",\"total_money\":\"499\",\"coupon_money\":\"0\",\"pay_type\":\"0\",\"real_total_money\":\"0\",\"is_need_pay\":\"0\",\"pay_money\":\"0\",\"account_money\":\"0\",\"payed_status\":\"0\",\"pay_version\":\"0\",\"responsible_type\":\"0\",\"update_time\":\"0000-00-00T00:00:00\",\"create_time\":\"2017-10-20T16:36:55\",\"pay_time\":\"0000-00-00T00:00:00\",\"order_type\":\"0\",\"tps_eta\":\"0\",\"phone\":\"15884444895\",\"is_direct_way\":\"0\",\"driver_id\":\"\",\"is_solo\":\"0\",\"expect_arrival_time\":\"0\",\"timestamp\":\"2017-10-20T16:36:55\",\"db\":\"sofa_order\",\"table\":\"order_list\",\"optype\":\"i\",\"table_filter\":\"order_list\"}"; //要发送的内容
    private static final String BROKER_LIST = "127.0.0.1:9092"; //broker的地址和端口
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类

    @Test
    public void testProducer() throws IOException {
        Properties props = new Properties();
        props.put("serializer.class", SERIALIZER_CLASS);
        props.put("metadata.broker.list", BROKER_LIST);

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        //Send one message.
        BufferedReader br = new BufferedReader(new InputStreamReader(ProducerTest.class.getClassLoader().getResourceAsStream("test.txt")));
        String line;
        while ((line = br.readLine()) != null) {
            KeyedMessage<String, String> message =
                    new KeyedMessage<String, String>(TOPIC, line);
            producer.send(message);
        }

        producer.close();
        br.close();
    }


}