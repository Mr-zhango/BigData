package cn.myfreecloud.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: zhangyang
 * @date: 2019/6/27 12:32
 * @description:
 */
public class Hbase {


    public static void main(String[] args) throws Exception {

//        boolean student = isTableExist("staff");
//        System.out.println(student);

//        putData("student1", "1001", "info", "class", "1");
//        close(connection, admin);

//        createTable("student","info");


        //scanTableAll("student1");

        getColumnFamilyData("student1","1001","info","age");
    }


    /**
     * 检查表是否存在
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public static boolean isTableExist(String tableName) throws Exception {
        //获取连接
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        return b;
    }


    private static Admin admin = null;
    private static Connection connection = null;
    private static Configuration configuration = null;

    static {
        //使用HBaseConfiguration的单例方法实例化
        configuration = HBaseConfiguration.create();


        //configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            //获取管理对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建表操作
     *
     * @param tableName
     * @param columnFamily
     * @throws Exception
     */
    public static void createTable(String tableName, String... columnFamily) throws Exception {

        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            return;
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

                descriptor.addFamily(hColumnDescriptor);
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }


    /**
     * 删除表
     *
     * @param tableName
     * @throws Exception
     */
    public static void deleteTable(String tableName) throws Exception {

        //判断表是否存在
        if (isTableExist(tableName)) {
            //使表下线
            admin.disableTable(TableName.valueOf(tableName));
            //执行删除操作
            admin.deleteTable(TableName.valueOf(tableName));

            close(connection, admin);
            System.out.println(tableName + ":表已经删除");
        } else {
            System.out.println("表" + tableName + "不存在");
            return;
        }

    }

    /**
     * 添加数据
     *
     * @param tableName
     * @throws Exception
     */
    public static void putData(String tableName, String rowKey, String columnFamily, String columnName, String value) throws Exception {

        Table tableObject = connection.getTable(TableName.valueOf(tableName));

        //创建put数据的对象(同时设置rowKey)
        Put put = new Put(Bytes.toBytes(rowKey));
        //添加数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));

        tableObject.put(put);
    }

    /**
     * 删除数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param value
     * @throws Exception
     */
    public static void delete(String tableName, String rowKey, String columnFamily, String columnName, String value) throws Exception {

        //获取table对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //带s的方法,删除所有的版本(删除的干净,不存在遗留版本)
        //建议使用
        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        //不带s的方法,默认删除最新的版本,+时间戳
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));

        table.delete(delete);

    }

    /**
     * 查询全表数据
     *
     * @param tableName
     * @throws Exception
     */
    public static void scanTableAll(String tableName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
//        scan.setStartRow()
//        scan.setStopRow()
        ResultScanner resultScanner = table.getScanner(scan);


        for (Result result : resultScanner) {
            //rowKey 相同的
            byte[] row = result.getRow();
            System.out.println("RowKey,我是外面的:"+Bytes.toString(row));
            //多个属性和多个版本,多个cell
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(
                    "RowKey:"+Bytes.toString(CellUtil.cloneRow(cell))
                    +",ColumnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))
                    +",ColumnName:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                    +",ColumnValue:"+Bytes.toString(CellUtil.cloneValue(cell))
                );
            }

            table.close();
        }
    }


    /**
     *
     * 查询指定列族的数据
     * @param tableName
     * @throws Exception
     */
    public static void getColumnFamilyData(String tableName, String rowKey, String columnFamily, String columnName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        //指定列族
        //get.addFamily(Bytes.toBytes(columnFamily));
        //指定列族和属性
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        //默认使用最高版本
//        get.setMaxVersions();


        //获取数据:由于这里是唯一的一个rowKey所以只能有一行的数据
        Result result = table.get(get);

        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            System.out.println(
                    "RowKey:"+Bytes.toString(CellUtil.cloneRow(cell))
                            +",ColumnFamily:"+Bytes.toString(CellUtil.cloneFamily(cell))
                            +",ColumnName:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                            +",ColumnValue:"+Bytes.toString(CellUtil.cloneValue(cell))
            );
        }

        table.close();

    }

    /**
     * 统一关闭方法
     *
     * @param connection
     * @param admin
     */
    private static void close(Connection connection, Admin admin) {
        //关闭连接
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //关闭admin
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
