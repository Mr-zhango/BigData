package cn.myfreecloud.mapreduce.weblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.获取一行
        String line = value.toString();


        Boolean result = paeseLog(line,context);

        // 3.判断是否合法
        if(!result){
            return;
        }
        // 调用方法进行判断

        // 4.丢去不合法的数据

        // 5.写出合法的日志
        context.write(value, NullWritable.get());
    }

    /**
     * 判断日志是否合法
     * @param line
     * @param context
     * @return
     */
    private Boolean paeseLog(String line, Context context) {
        // 2.截取
        String[] filelds = line.split(" ");

        // 判断字段长度是否大于11,大于11才是算作合法的日志

        if(filelds.length > 11 ){
            // 3 记录合法次数
            context.getCounter("map", "true").increment(1);
            return true;
        }else {
            // 4 记录不合法的次数
            context.getCounter("map", "false").increment(1);
            return false;
        }

    }
}
