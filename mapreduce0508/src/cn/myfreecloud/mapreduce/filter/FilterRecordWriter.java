package cn.myfreecloud.mapreduce.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguOut = null;
    private FSDataOutputStream otherOut = null;

    public FilterRecordWriter(TaskAttemptContext job) {
        Configuration configuration = job.getConfiguration();

        try {
            // 获取文件系统
            FileSystem fileSystem = FileSystem.get(configuration);

            //创建两个文件的输出流
            atguiguOut = fileSystem.create(new Path("e:/output/atguigu.log"));

            otherOut = fileSystem.create(new Path("e:/output/other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        //区分输入的key是否包含atguigu
        if(key.toString().contains("atguigu")){
            atguiguOut.write(key.toString().getBytes());
        }else{
            otherOut.write(key.toString().getBytes());
        }

    }

    /**
     * 关流操作
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        if (atguiguOut != null) {
            atguiguOut.close();
        }

        if (otherOut != null) {
            otherOut.close();
        }
    }
}
