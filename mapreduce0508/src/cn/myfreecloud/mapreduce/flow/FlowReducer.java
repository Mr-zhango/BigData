package cn.myfreecloud.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //计算总的流量

        long sum_upFlow = 0;
        long sum_downFlow = 0;

        for (FlowBean value : values) {
            //计算上行流量
            sum_upFlow += value.getUpFlow();
            //计算下行流量
            sum_downFlow += value.getDownFlow();
        }

        context.write(key,new FlowBean(sum_upFlow,sum_downFlow));

    }
}
