package cn.myfreecloud.storm.uv;

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
 * @date: 2019/7/1 18:03
 * @description:
 */
public class UvSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;

    private BufferedReader bufferedReader;

    private SpoutOutputCollector collector = null;
    private String str = null;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;

        //读取文件
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/website.log")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void nextTuple() {
        //业务逻辑的实现
        //获取数据

        try {
            while(null != (str = bufferedReader.readLine())){
                collector.emit(new Values(str));

                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //截取

        //发送


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
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
