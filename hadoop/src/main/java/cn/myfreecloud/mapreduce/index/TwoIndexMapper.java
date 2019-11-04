package cn.myfreecloud.mapreduce.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    //key
    Text k = new Text();
    //value
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

//		1) 获取一行
        String line = value.toString();

//		2）截取--
        String[] fields = line.split("--");

        // 3)赋值key和value
        k.set(fields[0]);
        v.set(fields[1]);

//		4）写出
        context.write(k, v);
    }
}
