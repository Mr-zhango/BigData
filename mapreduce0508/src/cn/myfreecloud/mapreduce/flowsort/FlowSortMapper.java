package cn.myfreecloud.mapreduce.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean,Text> {

    //创建bean对象
    FlowSortBean flowSortBean = new FlowSortBean();

    Text v = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行数据
        String line = value.toString();

        //2.截取字段
        String[] fields = line.split("\t");

        //3.封装bean对象,以及获取电话号的key(电话号码)


        //上行流量
        long upFlow = Long.parseLong(fields[1]);

        //下行流量
        long downFlow = Long.parseLong(fields[2]);

        flowSortBean.set(upFlow, downFlow);

        //设置电话号码
        v.set(fields[0]);
        //4.写出去
        context.write(flowSortBean,v);
    }

}
