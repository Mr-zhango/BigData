package cn.myfreecloud.storm.pv;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/7/1 16:59
 * @description:
 */
public class PvSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;

    private BufferedReader reader;

    private SpoutOutputCollector collector = null;

    private String str = null;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        //读取文件
        this.collector = collector;

        try {
            try {
                reader = new BufferedReader(new InputStreamReader(new FileInputStream("e:/website.log"), "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        try {
            //发送读取文件的每一行
            while ((str = reader.readLine())!=null){
                // 开始发送数据
                collector.emit(new Values(str));

                //延时
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //发送
        //声明发送的字段
        declarer.declare(new Fields("log"));
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }


    public void ack(Object o) {

    }

    public void fail(Object o) {

    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
