package cn.myfreecloud.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordcountPartitioner extends Partitioner<Text, IntWritable> {


    /**
     *
     * @param key 一行的数据
     * @param intWritable
     * @param i 返回的分区号
     * @return
     */
    @Override
    public int getPartition(Text key, IntWritable intWritable, int i) {
        // 需求：按照单词首字母的ASCII的奇偶分区
        String line = key.toString();

        // 1 截取首字母
        String firword = line.substring(0, 1);

        // 2 转换成ASCII
        char[] charArray = firword.toCharArray();

        int result = charArray[0];

        // 3 按照奇偶分区
        if (result % 2 ==0 ) {
            return 0;
        }else {
            return 1;
        }
    }

}
