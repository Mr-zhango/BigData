package cn.myfreecloud.java.volatiletest;

import java.util.concurrent.TimeUnit;

class MyData {
    volatile int number = 0;

    public void setNumber() {
        this.number = 60;
    }

    //请注意，此时number前面是加了volatile关键字修饰的
    public void addPlusPlus() {
        number++;
    }
}

/**
 * 1.验证Volatile的可见性，
 * 1.1假设int number = 0 ；number变量之前没有添加volatile关键字修饰,没有可见性
 * 1.2添加了volatile可以解决可见性问题
 *
 * 2.验证volatile不保证原子性
 * 2.1什么是原子性：
 * 不可分割，完成性，业绩某个线程正在做某个业务时，中间不可以被加塞或者被分割。需要整体完成 要么同时成功要么同时失败。
 * 2.2volatile不保证原子性的案例
 *
 */
public class VolatileDemo {

    // 1.1假设int number = 0 ；number变量之前没有添加volatile关键字修饰,没有可见性
    public static void main(String[] args) {

        MyData myData = new MyData();

        // 开启了20个线程
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {

                // 每个线程加1000次
                for (int j = 1; j < 1000; j++) {
                    myData.addPlusPlus();
                }

            }, String.valueOf(i)).start();
        }
        // 需要等待上面的20个线程全部计算完后，再用main线程取得最终的结果值看是多少
        while(Thread.activeCount() >2){
            Thread.yield();
        }
        //main	 finally number value: 19504
        //main	 finally number value: 19857
        System.out.println(Thread.currentThread().getName() + "\t finally number value: " + myData.number);
        //seeOkByVolatile();
    }

    // 验证Volatile的可见性 开始
    // Volatile可以保证可见性,及时通知其他线程,主物理内存的值已经被修改
    private static void seeOkByVolatile() {
        MyData myData = new MyData();//资源类

        new Thread(() -> {
            System.out.println(Thread.currentThread().getName() + "\t come in");
            try {
                // 线程等待3s
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            myData.setNumber();
        }, "Thread A").start();
        // 第二个线程就是main线程
        while (myData.number == 0) {
        // main线程一致在这里等待循环，直到number值不再为0
        }
        System.out.println(Thread.currentThread().getName() +"\t mission is over, main get number value: " + myData.number);
    }
    //验证Volatile的可见性 结束


}