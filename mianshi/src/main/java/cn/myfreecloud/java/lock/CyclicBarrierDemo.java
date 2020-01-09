package cn.myfreecloud.java.lock;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * 凑齐7个龙珠才能召唤神龙
 */
public class CyclicBarrierDemo {
    public static void main(String[] args) {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(7,() -> {
            System.out.println("******************召唤神龙******************");
        });


        for (int i = 1; i <= 7; i++) {
            final int tempInt = i;
            new Thread(() ->{
                System.out.println(Thread.currentThread().getName() + "\t 收集到第"+ tempInt +"颗龙珠");
                try {
                    // 先到的被阻塞
                    cyclicBarrier.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }

            },String.valueOf(i)).start();
        }
    }
}
