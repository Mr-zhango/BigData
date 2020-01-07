package cn.myfreecloud.java.cas;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ABA 问题的演示
 */
public class ABAQuestion {


    static AtomicReference<Integer> atomicReference = new AtomicReference<>(100);

    public static void main(String[] args) {

        new Thread(() -> {
            atomicReference.compareAndSet(100, 101);
            atomicReference.compareAndSet(101, 100);
        }, "t1").start();


        new Thread(() -> {
            try {
                // 线程等待1s 保证t1线程已经完成了ABA操作
                TimeUnit.SECONDS.sleep(1);
                System.out.println(atomicReference.compareAndSet(100, 2020) + "\t" + atomicReference.get());
            } catch (Exception e) {
                e.printStackTrace();
            }


        }, "t2").start();
    }
}
