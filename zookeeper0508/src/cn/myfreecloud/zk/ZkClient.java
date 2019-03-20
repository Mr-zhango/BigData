package cn.myfreecloud.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ZkClient {

    //连接zk的地址以及端口号配置
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";

    //设置会话的超时时间
    private int sessionTimeout = 2000;

    ZooKeeper zkClient = null;
    //1.创建一个客户端


    @Before
    public void initZk() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //监听发生后触发的事件
                System.out.println("事件类型:"+event.getType() +"--"+"事件路径:"+event.getPath());

                try {
                    //这个文件夹下发生事件之后继续注册监听
                    //atguigu文件夹再次注册结果
                    Stat exists = zkClient.exists("/atguigu", true);

                    //atguigu文件夹获取子节点注册结果
                    List<String> children = zkClient.getChildren("/", true);

                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });

    }


    //2.创建子节点
    @Test
    public void createNode() throws Exception {


        /**
         * path:创建的路径
         * path:创建节点存储的数据
         * acl:创建节点后具有的权限
         * createMode:节点类型
         *
         */
        String createResult = zkClient.create("/atguigu", "ss.video".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println("Zookeeper客户端创建结果:"+createResult);


    }

    //3.获取节点的数据
    @Test
    public void getChild() throws Exception {


        /**
         * path:想要获取的路径
         * watch:是否要进行监听
         *
         * 返回所有的子节点
         *
         */
        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }

        //进程等待
        Thread.sleep(Long.MAX_VALUE);
    }

    //4.判断节点是否存在
    @Test
    public void isVoid() throws Exception {


        /**
         * path:想要查看的路径
         * watch:是否要进行监听
         *
         * 返回所有的子节点
         *
         */

        Stat exists = zkClient.exists("/atguigu", true);

        System.out.println(exists == null ? "节点不存在":"节点存在");

        //进程等待
        Thread.sleep(Long.MAX_VALUE);
    }
}
