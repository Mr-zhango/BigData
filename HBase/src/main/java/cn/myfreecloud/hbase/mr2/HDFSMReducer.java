package cn.myfreecloud.hbase.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author: zhangyang
 * @date: 2019/6/27 19:47
 * @description:
 */
public class HDFSMReducer extends TableReducer<NullWritable, Put,NullWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //不用处理,直接获取值写出
        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
