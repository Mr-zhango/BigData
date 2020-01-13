package cn.myfreecloud.java.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 线程池demo
 * 第四种获得多线程的方式,线程池
 */
public class MyThreadDemo {

    public static void main(String[] args) {
        // Array Arrays
        // Collection Collections
        // Executor Executors
        // ExecutorService threadPool = Executors.newFixedThreadPool(5); // 一池5个线程
        ExecutorService threadPool = Executors.newSingleThreadExecutor(); // 一池1个线程

        // 模拟10个用户来连接,每个用户就是一个外部的请求线程

        try {
            for (int i = 1; i <=20 ; i++) {
                threadPool.execute(()->{
                    System.out.println(Thread.currentThread().getName() + "\t 办理业务");
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdown();
        }

    }
}
