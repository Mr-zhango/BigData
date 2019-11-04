package cn.myfreecloud.mapreduce.filter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 获取一行
        String line = value.toString();

        k.set(line);

        // 写出
        context.write(k, NullWritable.get());
    }
}
