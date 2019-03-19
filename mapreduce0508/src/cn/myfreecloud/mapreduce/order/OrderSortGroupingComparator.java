package cn.myfreecloud.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderSortGroupingComparator extends WritableComparator {


    //重写比较的方法

    //写一个空参构造(指明进行比较的是谁)
    public OrderSortGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        //根据订单id号进行比较,看是否是同一个订单(同一个订单作为一组)
        return aBean.getOrderId().compareTo(bBean.getOrderId());
    }


}
