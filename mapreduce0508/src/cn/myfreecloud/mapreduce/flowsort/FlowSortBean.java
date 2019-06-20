package cn.myfreecloud.mapreduce.flowsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

/**
 * 必须实现WritableComparable接口
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {


    /**
     * Ctrl+B
     * 可以看见其中的父类以及定义的方法，还可以使用ctrl+左键来代替。
     * 但是这样的方法不能看见put方法，因此我们最好的解决方法就是使用：
     * Ctrl+Alt+B快捷键，这样就能查找到hashmap中的put方法了！
     * Ctrl+Alt+B
     */


    /**
     * 它是用来查找类和方法在哪里被使用的
     * Alt+F7
     */
    HashMap hashmap = new HashMap();
    private long upFlow;
    private long downFlow;
    private long sumFlow;


    /**
     * 必须要重写默认的比较方法(为了排序使用)
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowSortBean o) {
        //倒叙排列的实现
        return this.sumFlow > o.getSumFlow() ? -1 : 1;
    }

    /**
     * hadoop中实现序列化接口必须有一个空参构造(反序列化的时候使用)
     */
    // 反序列化时，需要空参构造
    public FlowSortBean() {
        super();
    }


    public FlowSortBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }


    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow ;
    }

    /**
     * 序列化方法(写进硬盘)
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化方法(从硬盘中读取数据)
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {

        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();

    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }



}