package cn.myfreecloud.storm.pv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/7/1 17:27
 * @description:
 */
public class PvSumBolt implements IRichBolt {

    private Map<Long,Integer> map  = new HashMap<Long, Integer>();



    public void execute(Tuple tuple) {
        //进行累加求和的逻辑实现
        Long threadId = tuple.getLong(0);

        Integer pvNumber = tuple.getInteger(1);

        //替换原来线程的pvNumber
        map.put(threadId,pvNumber);

        Iterator<Integer> iterator = map.values().iterator();

        int sum = 0;

        while (iterator.hasNext()){
            sum += iterator.next();
        }

        System.err.println("sum:"+sum);
    }



    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }



    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
