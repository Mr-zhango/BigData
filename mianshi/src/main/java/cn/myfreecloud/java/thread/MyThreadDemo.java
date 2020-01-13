package cn.myfreecloud.java.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 线程池demo jdk版的
 * 第四种获得多线程的方式,线程池
 */
public class MyThreadDemo {

    public static void main(String[] args) {
        // Array Arrays
        // Collection Collections
        // Executor Executors
        // ExecutorService threadPool = Executors.newFixedThreadPool(5); // 一池5个线程          执行长期的任务,性能比较高
        // ExecutorService threadPool = Executors.newSingleThreadExecutor(); // 一池1个线程      一个任务一个任务执行
        ExecutorService threadPool = Executors.newCachedThreadPool(); // 缓冲线程池              执行很多短期的异步任务

        // 模拟10个用户来连接,每个用户就是一个外部的请求线程

        try {
            for (int i = 1; i <=10 ; i++) {
                // execute() 方法,叫下一个客户
                threadPool.execute(()->{
                    System.out.println(Thread.currentThread().getName() + "\t 办理业务");
                });

                // 暂停200ms
                // try {TimeUnit.MILLISECONDS.sleep(200);} catch (Exception e) {e.printStackTrace();}
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdown();
        }

    }
}
