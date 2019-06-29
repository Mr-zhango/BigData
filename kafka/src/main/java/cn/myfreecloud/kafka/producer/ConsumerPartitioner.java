package cn.myfreecloud.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/6/29 12:05
 * @description:
 */
public class ConsumerPartitioner implements Partitioner {

    //private Map configMap = null;

    /**
     * 核心的的分区方法
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        //分区的配置信息
        //configMap = configs;
    }
}
