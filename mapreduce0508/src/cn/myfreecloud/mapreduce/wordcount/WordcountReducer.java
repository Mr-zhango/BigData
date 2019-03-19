package cn.myfreecloud.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //统计所有单词的个数

        int count = 0;
        //输出所有的单词
        for (IntWritable value : values) {

            count += value.get();
        }

        //输出单词个数

        context.write(key,new IntWritable(count));

    }

}