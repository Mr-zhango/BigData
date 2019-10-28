package cn.myfreecloud.storm.weblog;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * @author: zhangyang
 * @date: 2019/7/1 11:57
 * @description:  Spout 水管,数据的输入源
 */
public class WebLogSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;

    private BufferedReader bufferedReader;

    private SpoutOutputCollector collector = null;

    private String str = null;


    /**
     * 打开资源
     * 通过这个SpoutOutputCollector 把spout数据弹射到bolt
     *
     * @param map
     * @param topologyContext
     * @param collector
     */
    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {

        // 打开输入的文件
        try {

            this.collector = collector;

            this.bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("D:/website.log"), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 下一个元组
     */
    public void nextTuple() {

        //打开文件
        // 循环调用的方法
        try {
            //有数据就执行
            while ((str = this.bufferedReader.readLine()) != null) {
                // 发射出去
                collector.emit(new Values(str));

                // 休息1ms
				Thread.sleep(1000);
            }
        } catch (Exception e) {

        }
    }


    /**
     * 声明发送到下一级spout的数据类型
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 声明输出字段类型
        outputFieldsDeclarer.declare(new Fields("log"));

    }





    public void ack(Object o) {

    }

    public void fail(Object o) {

    }



    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }



    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
