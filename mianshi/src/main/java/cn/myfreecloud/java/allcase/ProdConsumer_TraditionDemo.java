package cn.myfreecloud.java.allcase;


import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 多线程编程的口诀
 *
 * 1    线程  操作(方法)  资源类
 * 2    判断  干活        通知
 * 3    防止虚假唤醒机制
 */
class ShareData {
    private int number = 0;
    private Lock lock = new ReentrantLock();

    private Condition condition = lock.newCondition();

    public void increment() throws Exception {

        // 加锁
        lock.lock();
        try {

            // 注意:多线程使用while来进行判断,不要使用if来进行判断
            // 1. 判断 防止虚假唤醒机制
            while (number != 0) {
                // 等待,不能生产
                condition.await();
            }
            // 2. 干活
            number++;

            System.out.println(Thread.currentThread().getName() + "\t" + number);

            // 3.通知唤醒
            condition.signalAll();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放锁
            lock.unlock();
        }

    }


    public void decrement() throws Exception {

        // 加锁
        lock.lock();
        try {

            // 1. 判断 防止虚假唤醒机制
            while (number == 0) {
                // 等待,不能生产
                condition.await();
            }
            // 2. 干活
            number--;

            System.out.println(Thread.currentThread().getName() + "\t" + number);

            // 3.通知唤醒
            condition.signalAll();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放锁
            lock.unlock();
        }

    }
}

/**
 * 题目:一个初始值为0的变量,两个线程对其交替操作,一个加1衣蛾减1,来5轮
 * <p>
 * 1.线程操作资源类
 */
public class ProdConsumer_TraditionDemo {
    public static void main(String[] args) {
        ShareData shareData = new ShareData();

        new Thread(()->{

            for (int i = 1; i <= 5 ; i++) {
                try {
                    shareData.increment();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },"AAA").start();


        new Thread(()->{

            for (int i = 1; i <= 5 ; i++) {
                try {
                    shareData.decrement();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },"BBB").start();



        new Thread(()->{

            for (int i = 1; i <= 5 ; i++) {
                try {
                    shareData.increment();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },"CCC").start();

        new Thread(()->{

            for (int i = 1; i <= 5 ; i++) {
                try {
                    shareData.decrement();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        },"DDD").start();

    }






    //AAA	1
    //BBB	0
    //AAA	1
    //BBB	0
    //AAA	1
    //BBB	0
    //AAA	1
    //BBB	0
    //AAA	1
    //BBB	0
}
