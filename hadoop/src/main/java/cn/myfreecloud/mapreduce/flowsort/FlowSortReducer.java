package cn.myfreecloud.mapreduce.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * FlowSortBean, Text 输入(Text:电话号码)
 *
 */
public class FlowSortReducer extends Reducer<FlowSortBean, Text,Text,FlowSortBean> {

    @Override
    protected void reduce(FlowSortBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Text v = values.iterator().next();

        context.write(v,bean);
    }
}
