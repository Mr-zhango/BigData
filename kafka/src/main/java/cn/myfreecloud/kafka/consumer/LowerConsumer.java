package cn.myfreecloud.kafka.consumer;


import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author: zhangyang
 * @date: 2019/6/29 14:11
 * @description: 低级API操作KAFKA
 *
 * 根据指定的Topic,Partion,offset,来获取数据
 */
public class LowerConsumer {
    public static void main(String[] args) {
        //定义参数
        ArrayList<String> brockers = new ArrayList<>();

        brockers.add("hadoop102");
        brockers.add("hadoop103");
        brockers.add("hadoop104");


        //定义端口号
        int port = 9092;

        //主题
        String topic = "second";

        //分区
        int partition = 0;

        //offset 偏移量
        long offset = 2;

        LowerConsumer lowerConsumer = new LowerConsumer();

        lowerConsumer.getData(brockers,port,topic,partition,offset);
    }


    /**
     * 查找分区的leader
     * @param brokers
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic , int partition){

        //获取分区leader的消费者对象
        for (String broker : brokers) {


            SimpleConsumer getLeader = new SimpleConsumer(broker, port, 1000, 1024 * 4, "getLeader");

            //创建一个主题元数据信息请求
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));

            //获取主题元数据返回值
            TopicMetadataResponse topicMetadataResponse = getLeader.send(topicMetadataRequest);

            //解析元数据返回值
            List<TopicMetadata> topicMetadata = topicMetadataResponse.topicsMetadata();

            //遍历主题topic元数据
            for (TopicMetadata topicMetadatum : topicMetadata) {
                //获取多个分区的元数据信息
                List<PartitionMetadata> partitionsMetadata = topicMetadatum.partitionsMetadata();

                //遍历分区获取元数据信息
                for (PartitionMetadata partitionMetadata : partitionsMetadata) {
                    if(partition == partitionMetadata.partitionId()){
                        return partitionMetadata.leader();
                    }
                }
            }

            //没有找到领导,3个分区全部挂了
            return null;

        }

        return null;
    }

    /**
     * 获取数据
     */
    private void getData(List<String> brokers, int port, String topic , int partition,long offset){
        //获取分区leader
        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if(leader == null){
            return;
        }else {
            //获取数据的消费对象
            String leaderHost = leader.host();
            SimpleConsumer simpleConsumer = new SimpleConsumer(leaderHost, port, 1000, 1024 * 4, "getData");

            FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000000).build();

            FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);

            //解析返回值
            ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);

            for (MessageAndOffset messageAndOffset : messageAndOffsets) {
                long offset1 = messageAndOffset.offset();

                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];

                payload.get(bytes);

                System.out.println(offset1 + "-----------"+ new String(bytes));

            }
        }
    }
}
