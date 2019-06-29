package cn.myfreecloud.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: zhangyang
 * @date: 2019/6/27 19:47
 * @description:
 */
public class HDFSMDriver extends Configuration implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        int run = ToolRunner.run(configuration, new HDFSMDriver(), args);

        System.exit(run);
    }

    private Configuration configuration = null;


    @Override
    public int run(String[] args) throws Exception {
        //获取Job对象
        Job job = Job.getInstance(configuration);
        //
        job.setJarByClass(HDFSMDriver.class);

        //设置mapper
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);


        //设置reducer
        TableMapReduceUtil.initTableReducerJob("fruit2",
                HDFSMReducer.class,
                job);

        //设置输入路径
        FileInputFormat.setInputPaths(job,args[0]);
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
