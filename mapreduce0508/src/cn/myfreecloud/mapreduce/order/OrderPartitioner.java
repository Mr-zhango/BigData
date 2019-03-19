package cn.myfreecloud.mapreduce.order;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {

        //分区算法(根据订单号进行分区)   按照bean的hashCode进行分区
        //这里 & Integer.MAX_VALUE没什么用处,保证至少一个分区
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE ) %  i;
    }
}
