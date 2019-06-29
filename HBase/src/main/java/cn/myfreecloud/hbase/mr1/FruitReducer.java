package cn.myfreecloud.hbase.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author: zhangyang
 * @date: 2019/6/27 18:31
 * @description: 尽管没有用reducer,但是reducer中有输出格式化,写到HBase中所以也需要reducer
 */
public class FruitReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {


    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

        //循环写出
        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }

    }
}
