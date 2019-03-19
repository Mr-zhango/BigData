package cn.myfreecloud.mapreduce.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable , Text,OrderBean , NullWritable> {


    OrderBean orderBean = new OrderBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.读取数据
        String line = value.toString();

        //2.切割数据

        String[] fields = line.split("\t");

        //3.封装Bean对象
        orderBean.setOrderId(fields[0]);
        orderBean.setPrice(Double.parseDouble(fields[2]));

        //4.写出数据

        context.write(orderBean,NullWritable.get());
        //然后把map的数据传递到reducer里面去
    }
}
