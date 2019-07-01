package cn.myfreecloud.storm.pv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/7/1 17:12
 * @description:
 */
public class PvBolt1 implements IRichBolt {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector = null;

    private String str = null;

    private int pvNumber = 0;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        //获取collector
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        //获取数据
        String line = tuple.getString(0);

        //截取
        String[] split = line.split("\t");

        String sessionId = split[1];
        if(sessionId!=null){
            //局部的累加
            pvNumber++;
            //输出 记得对不同的线程进行区分,加上线程的id
            collector.emit(new Values(Thread.currentThread().getId(),pvNumber));
        }

        System.err.println(new Values(Thread.currentThread().getId()+"pvNumber:"+pvNumber));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //声明输出的字段
        declarer.declare(new Fields("threadid","pvNumber"));
    }


    public void cleanup() {

    }



    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
