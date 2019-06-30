package cn.myfreecloud.hbase.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author: zhangyang
 * @date: 2019/6/29 23:47
 * @description:
 */
public class WeiBoUtil {


    //创建hbase的连接
    private static Configuration configuration = HBaseConfiguration.create();

    //静态代码块,在本类初始化的时候就加载这里的配置
    static {
        //设置zookeeper的连接
        configuration.set("hbase.zookeeper.quorum", "192.168.1.20");
    }

    //创建命名空间
    public static void createNamespace(String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        //创建NameSpace的描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        //创建操作
        admin.createNamespace(namespaceDescriptor);

        //关闭资源
        admin.close();
        connection.close();

    }

    //创建表
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //循环添加列族
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        admin.createTable(hTableDescriptor);

        //关闭资源
        admin.close();
        connection.close();

    }

    //发布微博

    /**
     *
     * @param uid
     * @param content
     * @throws IOException
     */
    public static void createData(String uid,String content) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取三张表对象
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));

        Table relationTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));

        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));

        long ts = System.currentTimeMillis();

        String rowKey = uid + "_" + ts;

        //生成put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("content"),Bytes.toBytes(content));

        contTable.put(put);

        //获取关系表中的fans
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relationTable.get(get);

        Cell[] cells = result.rawCells();

        if(cells.length <= 0){
            return;
        }


        //更新fans收件箱表
        ArrayList<Put> putArrayList = new ArrayList<>();

        for (Cell cell : cells) {
            byte[] bytes = CellUtil.cloneQualifier(cell);

            Put inboxPut = new Put(bytes);

            inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid),ts, Bytes.toBytes(rowKey));

            putArrayList.add(inboxPut);

        }

        inboxTable.put(putArrayList);

        //关闭资源

        inboxTable.close();
        relationTable.close();
        contTable.close();

        connection.close();
    }

    //关注用户
    public static void addAttend(String uid,String... uids) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取三张表对象
        Table contTable = connection.getTable(TableName.valueOf(Constant.CONTENT));

        Table relationTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));

        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));

        //创建操作者的put对象
        Put relationPut = new Put(Bytes.toBytes(uid));

        ArrayList<Put> puts = new ArrayList<>();



        for (String s : uids) {
            relationPut.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(s),Bytes.toBytes(s));

            //创建被关注者的put对象
            Put fansPut = new Put(Bytes.toBytes(s));
            fansPut.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid), Bytes.toBytes(uid));
            puts.add(fansPut);
        }

        puts.add(relationPut);

        relationTable.put(puts);

        Put inboxPut = new Put(Bytes.toBytes(uid));

        //获取内容表中被关注者的rowKey
        for (String s : uids) {
            Scan scan = new Scan(Bytes.toBytes(s),Bytes.toBytes(s+"|"));

            ResultScanner results = contTable.getScanner(scan);
            for (Result result : results) {

                String rowKey = Bytes.toString(result.getRow());

                String[] split = rowKey.split("_");


                byte[] row = result.getRow();

                //使用发布微博时候的时间戳
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s),Long.parseLong(split[1]),row);
            }
        }

        inboxTable.put(inboxPut);

        inboxTable.close();
        relationTable.close();
        contTable.close();

        connection.close();
    }


    //取消关注用户

    //获取微博内容(初始化页面)

    //获取微博内容(查看某个人所有的微博内容)
}
