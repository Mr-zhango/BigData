package cn.myfreecloud.storm.weblog;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author: zhangyang
 * @date: 2019/7/1 14:13
 * @description: 链接spout和bolt的拓扑驱动
 */
public class WebLogMain {
    public static void main(String[] args) {
        //1.创建拓扑 拼接spout和bolt

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // 一个线程读取数据
        topologyBuilder.setSpout("WebLogSpout",new WebLogSpout(),1);
        // 两个线程处理数据
        topologyBuilder.setBolt("WebLogBolt",new WebLogBolt(),2).shuffleGrouping("WebLogSpout");

        //2.创建配置信息对象
        Config config = new Config();

        // 设置开启的worker数目
        config.setNumWorkers(2);

        //3.提交程序
        //在集群上运行
        if(args.length > 0){
            try {
                StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            //在本地运行
            LocalCluster localCluster = new LocalCluster();
            // 提交的任务的拓扑名称
            localCluster.submitTopology("webtopology",config,topologyBuilder.createTopology());
        }
    }
}
