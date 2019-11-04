package cn.myfreecloud.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {


    private String orderId; //订单id
    private Double price;   //商品价格


    /**
     * 排序的方法
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        //进行两次排序
        //1.按照id号进行排序
        int result = this.orderId.compareTo(o.getOrderId());
        //返回0是相等,返回-1是小于.返回1是大于
        //对相同的id号码再次进行排序
        if(result == 0){
            //2.按照价格进行倒叙排序
            result = this.price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    /**
     * 反序列化方法
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }


    /**
     * 空参构造必须有
     */
    public OrderBean() {
    }

    /**
     * 带参构造
     *
     * @param orderId
     * @param price
     */
    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }


    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
