package cn.myfreecloud.storm.uv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/7/1 18:20
 * @description:
 */
public class UvSumBolt implements IRichBolt {

    private Map<String,Integer> map = new HashMap<String,Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        //获取数据
        String ip = tuple.getString(0);

        Integer num = tuple.getInteger(1);

        //进行去重
        if(map.containsKey(ip)){
            Integer count = map.get(ip);
            map.put(ip,count+num);
        }else{
            map.put(ip,num);
        }

        //打印数据

        System.err.println("ip:"+ip+"number:"+map.get(ip));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
