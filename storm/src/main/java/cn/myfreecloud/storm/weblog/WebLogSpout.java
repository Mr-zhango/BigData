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
 * @description:
 */
public class WebLogSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;

    private BufferedReader bufferedReader;

    private SpoutOutputCollector collector = null;
    private String str = null;


    @SuppressWarnings("rawtypes")
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {

        // 打开输入的文件
        try {
            this.collector = collector;

            this.bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("e:/website.log"), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public void nextTuple() {

        //打开文件
        // 循环调用的方法
        try {
            //有数据就执行
            while ((str = this.bufferedReader.readLine()) != null) {
                // 发射出去
                collector.emit(new Values(str));

				Thread.sleep(1000);
            }
        } catch (Exception e) {

        }
    }



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
