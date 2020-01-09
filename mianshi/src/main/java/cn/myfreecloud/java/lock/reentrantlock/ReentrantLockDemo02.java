package cn.myfreecloud.java.lock.reentrantlock;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本案例说明 ReentrantLock 是一个典型的可重用锁
 */

class ReentrantPhone implements Runnable {
    public synchronized void sendSMS() throws Exception {
        System.out.println(Thread.currentThread().getId() + "\t invoked sendSMS()");
        sendEmail();
    }

    public synchronized void sendEmail() throws Exception {
        System.out.println(Thread.currentThread().getId() + "\t *****************invoked sendEmail()");
    }

    /**
     * -------------------------------------
     **/
    Lock lock = new ReentrantLock();

    @Override
    public void run() {
        get();
    }

    private void get() {
        // 注意,加锁的次数和解锁的次数一定要匹配,否则会导致解锁失败,程序卡在这里,不会结束
        // lock.lock();
        lock.lock();
        try {
            System.out.println(Thread.currentThread().getId() + "\t invoked get()");
            set();
        } finally {
            // lock.unlock();
            lock.unlock();
        }
    }

    private void set() {
        lock.lock();
        try {
            System.out.println(Thread.currentThread().getId() + "\t *************** invoked set()");
        } finally {
            lock.unlock();
        }
    }
}

public class ReentrantLockDemo02 {

    public static void main(String[] args) {

        //13	 invoked sendSMS()
        //13	 *****************invoked sendEmail()
        //14	 invoked sendSMS()
        //14	 *****************invoked sendEmail()
        ReentrantPhone reentrantphone = new ReentrantPhone();
        new Thread(
                () -> {
                    try {
                        reentrantphone.sendSMS();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "t1").start();


        new Thread(
                () -> {
                    try {
                        reentrantphone.sendSMS();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "t1").start();


        try {
            // 线程等待1s
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {
            e.printStackTrace();
        }


        System.out.println("-----------------------------------------");
        Thread t3 = new Thread(reentrantphone);

        Thread t4 = new Thread(reentrantphone);

        t3.start();
        t4.start();
    }

}
