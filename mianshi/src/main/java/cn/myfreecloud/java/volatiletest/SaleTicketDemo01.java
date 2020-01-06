package cn.myfreecloud.java.volatiletest;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 使用JUC java.util.current java并发包
 * 3个售票员,卖出30张票
 * 使用多线程实现
 */

// 资源类
class Ticket {
    // 现在有30张票
    private int number = 30;

    Lock lock = new ReentrantLock();

    // 卖票方法
    public void sale() {
        // 加锁
        lock.lock();
        try {
            if (number > 0) {
                // 打印当前线程名称
                System.out.println(Thread.currentThread().getName() + "\t 卖出第:" + (number--) + "\t还剩下:" + number);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
}

public class SaleTicketDemo01 {
    public static void main(String[] args) {

        // 创建资源
        Ticket ticket = new Ticket();

        // lambda 表达式的写法
        new Thread(() -> {for (int i = 1; i < 40; i++) {ticket.sale();}}, "A").start();
        new Thread(() -> {for (int i = 1; i < 40; i++) {ticket.sale();}}, "B").start();
        new Thread(() -> {for (int i = 1; i < 40; i++) {ticket.sale();}}, "C").start();

        // 创建三个线程
//       new Thread(new Runnable() {
//           @Override
//           public void run() {
//                for (int i = 1; i < 40 ; i++){
//                    ticket.sale();
//                }
//           }
//       },"A").start();
    }
}
