package cn.myfreecloud.zkcasse;


import org.apache.zookeeper.*;


/**
 *
 */
public class ZkServer {


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

            }
        });
    }

    //2.注册

    public void regist(String hostName) throws Exception {
        String create = zk.create(parentNode + "/server", hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);


        System.out.println(hostName +"is online"+ create);
    }

    //3.业务逻辑
    public void business() throws Exception {
        System.out.println("正式业务开始");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        //1.获取连接
        ZkServer zkServer = new ZkServer();
        zkServer.getConnect();
        //2.注册
        zkServer.regist(args[0]);
        //3.实现业务逻辑
        zkServer.business();

    }


}
