package com.z.logmake.test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.z.logmake.AnalyticsEngineSDK;

public class Test {
    private static Random random = new Random(System.currentTimeMillis());
    private static Set<Order> orders = new HashSet<>();

    public static void main(String[] args) throws InterruptedException {
        Order order = null; 
        while (true) {
            order = getSuccessOrder();
            // 发送订单付款行为数据
            AnalyticsEngineSDK.onChargeSuccess(order.orderId, order.memberId);
            Thread.sleep(random.nextInt(500));
            if (random.nextInt(100) > 75) {
                // 25%的订单发生退款行为
                order = getRefundOrder();
                if (order != null) {
                    // 发送订单退款行为数据
                    AnalyticsEngineSDK.onChargeRefund(order.orderId, order.memberId);
                    Thread.sleep(random.nextInt(500));
                }
            }
        }
    }

    private static Order getSuccessOrder() {
        while (true) {
            int orderId = random.nextInt(Math.max(200000, orders.size() * 2));
            Order order = new Order();
            order.orderId = "orderid" + orderId;
            if (!orders.contains(order)) {
                // 该order是一个新的order对象
                order.memberId = "itguigu" + random.nextInt(1000);
                orders.add(order);
                return order;
            }
        }
    }

    private static Order getRefundOrder() {
        int count = 0;
        Order[] os = orders.toArray(new Order[0]);
        while (true) {
            count++;
            int index = random.nextInt(os.length); // 获取下标位置
            Order order = os[index]; // 获取对应下标位置的数据
            if (!order.refund) {
                order.refund = true; // 设置为已经退款操作
                return order;
            } else if (count >= os.length) {
                // 设置最多重试次数
                return null;
            }
        }
    }

    static class Order {
        public String orderId;
        public String memberId;
        public boolean refund = false;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((orderId == null) ? 0 : orderId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Order other = (Order) obj;
            if (orderId == null) {
                if (other.orderId != null)
                    return false;
            } else if (!orderId.equals(other.orderId))
                return false;
            return true;
        }
    }
}
