package cn.myfreecloud.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 收入数据:
 * LongWritable:行号
 * Text:每一行的内容
 *
 * 输出数据:
 * Text:手机号
 * FlowBean:流量对象
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    //创建bean对象
    FlowBean flowBean = new FlowBean();

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行数据
        String line = value.toString();

        //2.截取
        String[] fields = line.split("\t");

        //3.封装bean对象,以及获取电话号的key(电话号码)

        //手机号
        String phoneNum = fields[1];

        //上行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);

        //下行流量
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        flowBean.set(upFlow, downFlow);

        k.set(phoneNum);
        //4.写出去
        context.write(k, flowBean);
    }

}
