package cn.myfreecloud.java.lock;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *  抢停车位模型,车排队进入停车场
 *  Semaphore 信号量,主要用于两个目的,一个是用户多个共享资源的互斥使用,另一个是用户控制并发线程数的控制
 */
public class SemaphoreDmo {
    public static void main(String[] args) {

        // 模拟3个停车场
        Semaphore semaphore = new Semaphore(3);


        // 模拟6部车
        for (int i = 1; i <= 6; i++) {
            final int tempInt = i;
            new Thread(() ->{

                try {
                    // 抢占
                    semaphore.acquire();
                    System.out.println(Thread.currentThread().getName() + "\t 抢到车位 in ");
                    // 暂停5s
                    try {TimeUnit.SECONDS.sleep(3);} catch (Exception e) {e.printStackTrace();}
                    System.out.println(Thread.currentThread().getName() + "\t 停车3s后离开车位 out");

                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    // 释放停车位
                    semaphore.release();
                }

            },String.valueOf(i)).start();
        }
    }
}
