package cn.myfreecloud.java.cas;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * cas:
 * 比较并交换 compareAndSet()
 * 相当于git的提交,只有最新版本的才能提交成功
 */
public class CASDemo {
    public static void main(String[] args) {

        AtomicInteger atomicInteger = new AtomicInteger(5);

        // main do thing......

        // 2019
        System.out.println(atomicInteger.compareAndSet(5, 2019) + "\t current value:" + atomicInteger.get());
        // 2019
        System.out.println(atomicInteger.compareAndSet(5, 2020) + "\t current value:" + atomicInteger.get());

    }
}
