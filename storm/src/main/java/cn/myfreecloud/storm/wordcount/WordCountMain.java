package cn.myfreecloud.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author: zhangyang
 * @date: 2019/7/1 16:25
 * @description:
 */
public class WordCountMain {
    public static void main(String[] args) {
        //1.创建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("WordCountSpout",new WordCountSpout(),1);

        //数据处理,bolt阶段
        //先进行在进行处理

        //设置4个线程,提高切割阶段的并行度
        builder.setBolt("WordCountSplitBolt",new WordCountSplitBolt(),4).fieldsGrouping("WordCountSpout",new Fields("love"));

        //进行汇总 汇总操作最好单线程进行,防止统计出现问题                                                                   按照word进行分组也就是love
        builder.setBolt("WordCountBolt",new WordCountBolt(),1).fieldsGrouping("WordCountSplitBolt",new Fields("word"));

        //2.创建配置信息
        Config config = new Config();

//        config.setNumWorkers(2);

        //3.提交程序
        //3.提交程序
        //在集群上运行
        if(args.length > 0){
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }else{
            //在本地运行
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordCountTopology",config,builder.createTopology());
        }
    }
}
