package cn.myfreecloud.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.net.URI;

public class HDFSClient {

    private static Log logger = LogFactory.getLog(HDFSClient.class);


    public static void main(String[] args) throws Exception {
        System.out.println("------开始向HDFS上上传文件-------");
        //文件系统的配置信息
        Configuration configuration = new Configuration();

        //设置默认nameNode的位置
        //configuration.set("fs.defaultFS", "hdfs://hadoop102:8020/");
        //1.获取文件系统
        //FileSystem fileSystem = FileSystem.get(configuration);

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration, "root");

        //2.本地文件拷贝到集群
        fileSystem.copyFromLocalFile(new Path("e:/xiyou.txt"), new Path("/user/input/xiyou.txt"));

        //3.关闭文件系统

        fileSystem.close();
        System.out.println("------文件上传结束-------");

    }

    //获取文件系统
    @Test
    public void getFileStstem() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "atguigu");

        //打印文件系统
        System.out.println(fileSystem.toString());
    }


    /**
     * 把文件上传到HDFS文件系统
     * @throws Exception
     */
    @Test
    public void putFileToHDFS() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "root");

        //执行上传命令(是否删除源文件)
        fileSystem.copyFromLocalFile(true,new Path("D:/access_20191106_112805.log"),new Path("/event_logs/2019/11/06"));
        //关闭资源

        //3.关闭文件系统

        fileSystem.close();
    }

    /**
     * 从HDFS文件系统中下载文件
     * @throws Exception
     */
    @Test
    public void getFileFromDFS() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "atguigu");

        //执行下载命令
        fileSystem.copyToLocalFile(false,new Path("/user/atguigu/bajie.txt"),new Path("e:/bajie.txt"),true);

        //关闭资源

        //3.关闭文件系统

        fileSystem.close();
    }

    /**
     * 在集群上创建目录
     */
    @Test
    public void mkdirAtDFS() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "root");

        //执行创建文件夹命令
        fileSystem.mkdirs(new Path("/input"));

        //关闭资源

        //3.关闭文件系统

        fileSystem.close();
    }

    /**
     * 在集群上删除文件夹
     */
    @Test
    public void deleteAtDFS() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "root");

        //执行删除命令
        //fileSystem.delete(new Path("/user/atguigu/output/xiaoxiong.txt"),true);
        fileSystem.delete(new Path("/event_logs/2019/11/06/access_20191106_112805.log"),true);
        //关闭资源
//        	access_20191106_112805.log

        //3.关闭文件系统

        fileSystem.close();
    }

    /**
     * 在集群上删除文件
     */
    @Test
    public void deleteFilrAtDFS() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "root");

        //执行删除命令
        fileSystem.delete(new Path("/event_logs/"),true);
        //关闭资源

        //3.关闭文件系统

        fileSystem.close();
    }

    /**
     * 更改文件名称
     */
    @Test
    public void renameAtDFS() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "atguigu");

        //执行更改名称操作
        fileSystem.rename(new Path("/user/atguigu/xiyou.txt"),new Path("/user/atguigu/nhao.txt"));

        //关闭资源

        //3.关闭文件系统

        fileSystem.close();
    }
    /**
     * 查看文件详情
     */
    @Test
    public void readFileAtHDFS() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "root");

        //2.执行查看文件操作(true)表示进行迭代
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);

        //循环取出文件信息
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus status = locatedFileStatusRemoteIterator.next();

            //块的大小
            long blockSize = status.getBlockSize();
            System.out.println(blockSize);
            //文件的名字
            String name = status.getPath().getName();

            System.out.println(name);
            //文件的长度
            long len = status.getLen();

            System.out.println(len);

            //文件权限
            FsPermission permission = status.getPermission();
            System.out.println(permission);

            System.out.println("---------------------------------");
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println(blockLocation.getOffset());

                System.out.println("*****************");

                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }

            }

        }
        //3.关闭文件系统
        fileSystem.close();
    }


    /**
     * 获取文件夹和文件信息
     */
    @Test
    public void readFolderAtHDFS() throws Exception{
        //0创建配置信息对象
        Configuration configuration = new Configuration();

        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:8020/"), configuration , "atguigu");

        //判断是文件还是文件夹
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/user/atguigu"));

        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()){
                System.out.println("file-------"+fileStatus.getPath().getName());
            }else {
                System.out.println("directory-------"+fileStatus.getPath().getName());
            }
        }
        //3.关闭文件系统
        fileSystem.close();
    }
}
