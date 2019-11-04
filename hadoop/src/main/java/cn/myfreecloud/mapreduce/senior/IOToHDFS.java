package cn.myfreecloud.mapreduce.senior;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class IOToHDFS {
    /**
     * 本地文件上传到hdfs
     * @throws Exception
     */
    @Test
    public void putFileToHDFS() throws Exception{

        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "atguigu");

        //2.获取输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/user/atguigu/output/dongsi.txt"));

        //3.获取输入流
        FileInputStream fileInputStream = new FileInputStream(new File("e:/dongsi.txt"));

        //4.两流对接

        try {
            IOUtils.copyBytes(fileInputStream,fsDataOutputStream,configuration);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(fileInputStream);
            IOUtils.closeStream(fsDataOutputStream);
        }


        //5.关闭文件系统
        fileSystem.close();
    }


    @Test
    public void getFileFromHDFS() throws Exception{

        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "atguigu");

        //2.获取输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/user/atguigu/xiyou1.txt"));

        //3.获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("e:/xiyou1.txt"));

        //4.两流对接

        try {
            IOUtils.copyBytes(fsDataInputStream,fileOutputStream,configuration);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //g关流
            IOUtils.closeStream(fsDataInputStream);
            IOUtils.closeStream(fileOutputStream);
        }

        //5.关闭文件系统
        fileSystem.close();
    }


    /**
     * 下载超过128M的文件(两块)
     * @throws Exception
     */
    @Test
    public void getFileFromHDFSSeek1() throws Exception{

        //0.创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "atguigu");

        //2.获取输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/user/atguigu/input/hadoop-2.7.2.tar.gz"));

        //3.获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));

        //4.两流对接(只读取128M)
        byte[] buf = new byte[1024];
        //1024*1024*128 (128M)

        for (int i = 0; i < 1024 *128; i++) {
            fsDataInputStream.read(buf);
            fileOutputStream.write(buf);
        }

        try {
            //5.关流
            IOUtils.closeStream(fsDataInputStream);
            IOUtils.closeStream(fileOutputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //5.关闭文件系统
        fileSystem.close();
    }

    /**
     * 下载超过128M的文件(下载除了128M之后还多的数据)
     * @throws Exception
     */
    @Test
    public void getFileFromHDFSSeek2() throws Exception{

        //0.创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "atguigu");

        //2.获取输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/user/atguigu/input/hadoop-2.7.2.tar.gz"));

        //3.创建输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part2"));

        //4.两流对接-指向第二块数据的首地址

        //5.定位到128M

        byte[] buf = new byte[1024];
        //1024*1024*128 (128M)

        fsDataInputStream.seek(1024*1024*128);//128M的位置

        try {
            IOUtils.copyBytes(fsDataInputStream,fileOutputStream,configuration);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //5.关流
            IOUtils.closeStream(fsDataInputStream);
            IOUtils.closeStream(fileOutputStream);
        }


        //5.关闭文件系统
        fileSystem.close();
    }

}
