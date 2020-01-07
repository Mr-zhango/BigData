package cn.myfreecloud.java.cas;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * ABA 问题的解决
 */
public class ABASolution {


    static AtomicReference<Integer> atomicReference = new AtomicReference<>(100);
    static AtomicStampedReference<Integer> atomicStampedReference = new AtomicStampedReference<>(100, 1);

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

        // 暂停一会线程
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        new Thread(() -> {
            // 初始版本号
            int stamp = atomicStampedReference.getStamp();
            //
            System.out.println(Thread.currentThread().getName() + "\t 第1次版本号:" + stamp);

            // 暂停一会线程
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            // t3 线程进行ABA操作
            atomicStampedReference.compareAndSet(100, 101, atomicStampedReference.getStamp(), atomicStampedReference.getStamp() + 1);
            System.out.println(Thread.currentThread().getName() + "\t 第2次版本号:" + atomicStampedReference.getStamp());

            atomicStampedReference.compareAndSet(101, 100, atomicStampedReference.getStamp(), atomicStampedReference.getStamp() + 1);
            System.out.println(Thread.currentThread().getName() + "\t 第3次版本号:" + atomicStampedReference.getStamp());

        }, "t3").start();


        new Thread(() -> {
            // 初始版本号
            int stamp = atomicStampedReference.getStamp();
            //
            System.out.println(Thread.currentThread().getName() + "\t 第1次版本号:" + stamp);

            // 暂停3st4线程,保证上面的t3线程完成了一次ABA操作
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (Exception e) {
                e.printStackTrace();
            }


            boolean result = atomicStampedReference.compareAndSet(100, 2020, stamp, stamp + 1);

            System.out.println(Thread.currentThread().getName() + "\t 是否修改成功:" + result + "\t 当前最新实际版本号:" + atomicStampedReference.getStamp());

            System.out.println((Thread.currentThread().getName() + "\t 当前实际最新值:" + atomicStampedReference.getReference()));

        }, "t4").start();

    }
}
