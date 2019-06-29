package cn.myfreecloud.hbase.mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: zhangyang
 * @date: 2019/6/27 18:31
 * @description:
 */
public class FruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     *
     * @param key rowKey
     * @param value rowKey对应的value
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //先对key进行反序列化
        Put put = new Put(key.get());

        //获取到一列数据
        Cell[] cells = value.rawCells();
        for (Cell cell : cells) {
            if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                put.add(cell);
            }
        }
        //写出去
        context.write(key,put);

    }
}
