package cn.myfreecloud.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/7/1 16:18
 * @description: 单词统计的结点:BOLT:阀门
 */
public class WordCountBolt extends BaseRichBolt {
    /**
     * key:单词 value:出现的次数
     */
    private Map<String,Integer> map = new HashMap<String, Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        /**
         * 获取传递过来的数据
         */
        String word = tuple.getString(0);

        Integer num = tuple.getInteger(1);

        //业务处理(进行去重)
        if(map.containsKey(word)){
            Integer count = map.get(word);
            //之前已经统计过该单词了,继续进行累加
            map.put(word,num+count);
        }else {
            //单词第一次出现
            map.put(word,num);
        }

        //打印数据
        System.err.println(Thread.currentThread().getId() + "------"+word +"----"+map.get(word));

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
