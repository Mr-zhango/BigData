package cn.myfreecloud.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



import java.io.IOException;

/**
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *
 * KEYIN:	 输入数据的key 文件的行号
 * VALUEIN:	每行的输入数据
 * KEYOUT:	每行的输出数据
 * VALUEOUT:输出数据的value类型
 *
 * @author zhangyang
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable longWritable, Text value, Context context) throws IOException,InterruptedException{

        //1.获取这一行的数据()把hadoop中的数据类型转换成java的数据类型
        String line = value.toString();

        //2.获取每一个单词
        String[] words = line.split(" ");

        for (String word : words) {
            //3.输出每一个单词
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
