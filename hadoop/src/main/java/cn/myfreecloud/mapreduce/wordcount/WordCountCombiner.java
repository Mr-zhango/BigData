package cn.myfreecloud.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        /**
         * 例输入为:
         * <a,1>  <a,1> <a,1>
         * <b,1>  <b,1>
         *  进行合并,直接输出
         *
         *  <a,3>
         *  <b,2>
         *  从而提高reduce的效率
         */
        //对相同的key的数据进行累加
        int count = 0;

        for (IntWritable value : values) {
            count += value.get();
        }

        //输出累加好的数据
        context.write(key,new IntWritable(count));


    }
}
