package cn.myfreecloud.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 一次性信息
 * @author zhangyang
 *
 */
public class Consistent {

    public static void main(String[] args) throws Exception{

        //System.out.println("hello");
        //文件系统的配置信息
        Configuration configuration = new Configuration();

        // 1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration, "atguigu");

        // 2.获取输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/xiyou1.txt"));

        // 3.具体的写上传数据
        fsDataOutputStream.write("hello".getBytes());

        // (永久存储数据)永久存储
        fsDataOutputStream.hflush();
        // 4.关闭资源
        fsDataOutputStream.close();

    }

}
