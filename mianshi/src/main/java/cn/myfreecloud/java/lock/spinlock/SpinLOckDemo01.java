package cn.myfreecloud.java.lock.spinlock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 目的:实现一个自旋锁
 * 自旋锁的好处:循环比较,直到比较获取成功为止,没有类似wait的阻塞
 *
 * 通过CAS操作完成自旋锁,A线程先进来调用myLock方法自己持有5s,B随后进来发现当前 线程只持有锁,不是null,
 * 所以只能通过自旋等待,直到A释放锁B后才能抢到.
 *
 *  自旋锁
 *  是指尝试获取锁的线程不会立即被阻塞,而是采用循环的方式尝试获取锁,这样的好处是减少线程上下文的切换,缺点是会消耗CPU的资源
 *
 */




public class SpinLOckDemo01 {

    // 原子引用线程
    AtomicReference<Thread> atomicReference = new AtomicReference<>();

    // 加锁
    public void myLock(){
        // 当前线程
        Thread thread = new Thread().currentThread();
        System.out.println(Thread.currentThread().getName()  + "\t come in ");

        // 自旋
        while(!atomicReference.compareAndSet(null,thread)){

        }

    }


    // 解锁
    public void myUnLock(){
        // 当前线程
        Thread thread = new Thread().currentThread();
        atomicReference.compareAndSet(thread,null);
        System.out.println(Thread.currentThread().getName() + " \t invocked myUnlock()");
    }
    public static void main(String[] args) {
        SpinLOckDemo01 spinLOckDemo01 = new SpinLOckDemo01();

        new Thread(
                () -> {
                    // 加锁
                    spinLOckDemo01.myLock();

                    // 暂停5s
                    try {TimeUnit.SECONDS.sleep(5);} catch (Exception e) {e.printStackTrace();}

                    // 解锁
                    spinLOckDemo01.myUnLock();
                }, "AA").start();

        // 暂停1s 保证AA线程首先启动
        try {TimeUnit.SECONDS.sleep(1);} catch (Exception e) {e.printStackTrace();}

        new Thread(
                () -> {
                    // 加锁
                    spinLOckDemo01.myLock();
                    try {TimeUnit.SECONDS.sleep(1);} catch (Exception e) {e.printStackTrace();}
                    spinLOckDemo01.myUnLock();
                }, "BB").start();

    }
}
