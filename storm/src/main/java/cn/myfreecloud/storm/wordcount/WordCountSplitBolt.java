package cn.myfreecloud.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/7/1 16:11
 * @description:
 */
public class WordCountSplitBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {

        //接收数据
        //1.获取数据
        String line = tuple.getString(0);
        //2.截取数据  I am ximengqing love jinlian
        String[] s = line.split(" ");

        //3.发出去

        for (String word : s) {
            collector.emit(new Values(word,1));
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //声明字段
        declarer.declare(new Fields("word","num"));
    }
}
