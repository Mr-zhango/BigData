package cn.myfreecloud.storm.weblog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/7/1 12:55
 * @description:
 */
public class WebLogBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector = null;
    private int line_num = 0;
    private String valueString = null;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple input) {
        //执行方法

        //1.获取数据
       // valueString = input.getStringByField("log");
        String string = input.getString(0);

        //www.myfreecloud.cn	VVVYH6Y4V4SFXZ56JIPDPB4V678	2017-08-07 08:40:51
        //2.切割数据
        String[] split = string.split("\t");

        String sesionId = split[1];
        //3.统计发送行数
        line_num++;

        //4.打印
        System.err.println(Thread.currentThread().getId() + "------------" +sesionId+"----" +"line_num:"+line_num);
    }

    public void cleanup() {
        //清除数据

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {

        //获取配置信息

        return null;
    }
}
