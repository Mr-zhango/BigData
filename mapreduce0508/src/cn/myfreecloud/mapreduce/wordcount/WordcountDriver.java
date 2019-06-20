package cn.myfreecloud.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 驱动主程序
 */
public class WordcountDriver {

    /**
     * driver:用来提交程序
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //   指定本程序的jar包所在的本地路径

        job.setJarByClass(WordcountDriver.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


//        //(对小文件进行了优化)
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
//        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m

        // 5 指定job的输入原始文件所在目录&输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


//        // 8.添加分区
//        job.setPartitionerClass(WordcountPartitioner.class);
//        //分配给两个reduce任务
//        job.setNumReduceTasks(2);
//
//        //9.关联Combiner
//        job.setCombinerClass(WordCountCombiner.class);

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
//		job.submit();
        boolean result = job.waitForCompletion(true);
        // 成功返回0,失败返回1
        System.exit(result?0:1);
    }
}
