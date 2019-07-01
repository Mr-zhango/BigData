package cn.myfreecloud.storm.uv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author: zhangyang
 * @date: 2019/7/1 18:27
 * @description:
 */
public class UvMain {
    public static void main(String[] args) {
        //1.创建拓扑

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("UvSpout", new UvSpout(), 1);

        builder.setBolt("UvSplitBolt", new UvSplitBolt(), 4).shuffleGrouping("UvSpout");

        //求和
        builder.setBolt("UvSumBolt", new UvSumBolt()).shuffleGrouping("UvSplitBolt");


        //2.创建配置信息对象
        Config config = new Config();

        //3.提交程序
        //在本地运行
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("UvSumTopology", config, builder.createTopology());
    }
}
