package cn.myfreecloud.mapreduce.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //因为前面的map阶段已经排过序了,所以在reduce阶段直接输出排在第一位的数据即可

        //写出bean数据
        context.write(key,NullWritable.get());
    }
}
