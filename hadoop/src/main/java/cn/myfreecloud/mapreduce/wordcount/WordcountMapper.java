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
 * KEYIN:LongWritable	 输入数据的key 文件的行号
 * VALUEIN:Text	每行的输入数据
 * KEYOUT:Text	输出数据的key:map的键
 * VALUEOUT:IntWritable 输出数据的value类型map的值
 *
 * @author zhangyang
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * hello world
     * atguigu atguigu
     * 业务逻辑写在map方法中
     * @param longWritable 输入数据行号
     * @param value 这一行的数据
     * @param context 上下文,帮助我们把输出的数据写入到下一个环节(这里写到缓存中去了)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable longWritable, Text value, Context context) throws IOException,InterruptedException{

        //1.获取这一行的数据()把hadoop中的数据类型转换成java的数据类型
        String line = value.toString();

        //2.获取每一个单词
        String[] words = line.split(" ");

        for (String word : words) {
            //3.输出每一个单词 数量是1个
            //使用context上下文工具输入到内存的缓冲区中去
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
