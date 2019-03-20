package cn.myfreecloud.zkcasse;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.ArrayList;
import java.util.List;

public class ZkClient {



    //连接zk的地址以及端口号配置
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";

    //设置会话的超时时间
    private int sessionTimeout = 2000;

    //1.创建一个客户端
    ZooKeeper zk = null;

    //定义一个父节点
    private String parentNode = "/servers";

    //1.获取连接
    public void getConnect() throws Exception {

        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                //监听发生后触发的事件
                System.out.println("事件类型:"+event.getType() +"--"+"事件路径:"+event.getPath());

                try {
                    //保证能循环执行
                    getServers();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }


    /**
     * 监听节点的变化
     * @throws Exception
     */
    public void getServers() throws Exception {
        //监听parentNode子节点
        List<String> children = zk.getChildren(parentNode, true);
        //
        ArrayList<String> servers = new ArrayList<>();

        //获取所有的子节点的信息
        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);

            servers.add(new String(data));
        }

        //打印servsers的数据
        System.out.println(servers);

    }


    public void bussiness() throws Exception {
        System.out.println("具体的业务逻辑实现");

    }
    public static void main(String[] args) throws Exception {
        //1.获取连接
        ZkClient zkClient = new ZkClient();
        zkClient.getConnect();

        //2.监听节点的变化
        zkClient.getServers();
        //3.实现自己的业务逻辑
        zkClient.bussiness();
    }
}
