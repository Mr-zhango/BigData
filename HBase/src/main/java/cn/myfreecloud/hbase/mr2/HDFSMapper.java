package cn.myfreecloud.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: zhangyang
 * @date: 2019/6/27 19:47
 * @description: mapper端直接生成Put类型,减小reducer端的压力
 */
public class HDFSMapper extends Mapper<LongWritable, Text,NullWritable, Put> {

    /**
     *
     * @param key 行号
     * @param value 本行的数据
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行数据
        String line = value.toString();
        //2.切割
        String[] split = line.split("\t");

        //3.封装put对象 设置
        Put put = new Put(Bytes.toBytes(split[0]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(split[1]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(split[1]));

        //4.写出去
        context.write(NullWritable.get(),put);
    }
}
