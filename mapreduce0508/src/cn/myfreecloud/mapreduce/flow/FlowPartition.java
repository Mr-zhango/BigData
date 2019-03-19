package cn.myfreecloud.mapreduce.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartition extends Partitioner<Text,FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean flowBean, int i) {

        // 根据电话号码的前三位是几来进行分区


        // substring()这个方法,包括左边,不包括右边
        // 拿到电话号码的前三位
        String phoneNum = key.toString().substring(0, 3);

        int partition = 4;

        // 2 判断是哪个省
        if ("136".equals(phoneNum)) {
            partition = 0;
        }else if ("137".equals(phoneNum)) {
            partition = 1;
        }else if ("138".equals(phoneNum)) {
            partition = 2;
        }else if ("139".equals(phoneNum)) {
            partition = 3;
        }

        return partition;
    }

}
