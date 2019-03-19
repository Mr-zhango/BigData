package cn.myfreecloud.mapreduce.index;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
//                                         行号     每一行的内容  输出:String Int
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取一行数据
        String line = value.toString();

        //2.截取字段
        String[] fields = line.split(" ");

        //3.获取文件的名称
        //首先获取文件系统的信息
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        Path path = fileSplit.getPath();
        String fileName = path.getName();


        /**
         * 普通for循环快捷键itar
         */
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];

            k.set(field+"--"+fileName);
            //输出
            context.write(k,new IntWritable(1));
        }



    }
}
