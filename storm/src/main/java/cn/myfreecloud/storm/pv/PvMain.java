package cn.myfreecloud.storm.pv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author: zhangyang
 * @date: 2019/7/1 17:38
 * @description:
 */
public class PvMain {
    public static void main(String[] args) {
        //1.创建拓扑

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("PvSpout", new PvSpout(), 1);

        topologyBuilder.setBolt("PvBolt1", new PvBolt1(), 4).shuffleGrouping("PvSpout");


        topologyBuilder.setBolt("PvSumBolt", new PvSumBolt()).shuffleGrouping("PvBolt1");


        //2.创建配置信息对象
        Config config = new Config();

        //3.提交程序
        //在本地运行
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("PvSumTopology", config, topologyBuilder.createTopology());

    }
}
