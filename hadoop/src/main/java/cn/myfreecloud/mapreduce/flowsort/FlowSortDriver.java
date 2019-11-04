package cn.myfreecloud.mapreduce.flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSortDriver {
    public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException{
        //3+2+1

        //1.获取配置信息
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        //2.获取jar的存储路径
        job.setJarByClass(FlowSortDriver.class);

        //3.关联map和reduce的class类
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        //4.设置map阶段输出的key的value的类型(根据Reduce来确定)
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);


        //5.设置最后输出的数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowSortBean.class);


        //6.设置输入数据的路径和输出数据的路径
        // 5 指定job的输入原始文件所在目录&输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0:1);
    }
}
