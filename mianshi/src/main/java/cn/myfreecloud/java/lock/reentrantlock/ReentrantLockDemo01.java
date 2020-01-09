package cn.myfreecloud.java.lock.reentrantlock;

/**
 *  本案例说明 synchronized 是一个典型的可重用锁
 */

// 手机类
class Phone {
    public synchronized void sendSMS() throws Exception{
        System.out.println(Thread.currentThread().getId() + "\t invoked sendSMS()");
        sendEmail();
    }

    public synchronized void sendEmail() throws Exception{
        System.out.println(Thread.currentThread().getId() + "\t *****************invoked sendEmail()");
    }
}


// ReentrantLock 可重用锁(又名递归锁) 默认是非公平的锁,参数写true是公平的锁
//synchronized 也是非公平锁

// 可重用锁(又名递归锁)
// 指的是同一个线程外层函数获得锁之后,内层递归函数仍然能够获取该锁的代码,在同一个线程在外层方法获取锁的时候,在进入内层方法会自动获取锁
// 也就是说,线程可以进入任何一个它已经拥有的锁所同步着的代码块
// ReentrantLock  和 synchronized 是典型的可重入锁(也叫递归锁),优势就是能够避免死锁
//
public class ReentrantLockDemo01 {

    public static void main(String[] args) {

        //13	 invoked sendSMS()
        //13	 *****************invoked sendEmail()
        //14	 invoked sendSMS()
        //14	 *****************invoked sendEmail()
        Phone phone = new Phone();
        new Thread(
                () -> {
                    try {
                        phone.sendSMS();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "t1").start();


        new Thread(
                () -> {
                    try {
                        phone.sendSMS();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "t1").start();
    }

}
