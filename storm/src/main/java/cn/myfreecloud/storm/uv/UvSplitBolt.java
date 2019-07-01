package cn.myfreecloud.storm.uv;

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
 * @date: 2019/7/1 18:14
 * @description:
 */
public class UvSplitBolt implements IRichBolt {


    private static final long serialVersionUID = 1L;
    private OutputCollector collector = null;
    private int line_num = 0;
    private String valueString = null;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        //获取数据
        String line = tuple.getString(0);

        String[] split = line.split("\t");

        //截取www.myfreecloud.cn	BBYH61456FGHHJ7JL89RG5VV9UYU7	2017-08-07 08:40:50	192.168.1.106
        String ip = split[3];

        //发送

        collector.emit(new Values(ip,1));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ip","num"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
