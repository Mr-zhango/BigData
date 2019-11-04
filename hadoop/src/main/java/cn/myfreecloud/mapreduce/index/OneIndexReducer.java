package cn.myfreecloud.mapreduce.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //对切片好的数据进行累加求和
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int counnt = 0;

        for (IntWritable value : values) {
            counnt += value.get();
        }

        //写出去
        context.write(key,new IntWritable(counnt));
    }
}
