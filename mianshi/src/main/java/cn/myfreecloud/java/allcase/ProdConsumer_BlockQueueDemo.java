package cn.myfreecloud.java.allcase;

/**
 *
 */

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
class MyResoure {
    // 标志位
    private volatile boolean FLAG = true;    // 默认开启,进行生产+消费

    //
    private AtomicInteger atomicInteger = new AtomicInteger();

    BlockingQueue<String> blockingQueue = null;

    public MyResoure(BlockingQueue<String> blockingQueue) {
        this.blockingQueue = blockingQueue;
        System.out.println(blockingQueue.getClass().getName());
    }

    public void myProd() throws Exception {
        String data = null;
        boolean retValue;
        while (FLAG) {
            data = atomicInteger.incrementAndGet() + "";
            retValue = blockingQueue.offer(data, 2L, TimeUnit.SECONDS);

            if (retValue) {
                System.out.println(Thread.currentThread().getName() + "\t 插入队列" + data + "成功");
            }else {
                System.out.println(Thread.currentThread().getName() + "\t 插入队列" + data + "失败");
            }

            TimeUnit.SECONDS.sleep(1);
        }

        System.out.println(Thread.currentThread().getName() + "\t 大老板叫停了,表示flag = false 生产动作结束了");
    }

    public void myConsumer() throws Exception{
        String result = null;
        while (FLAG){
            // 设置等待时间为2s钟
            result = blockingQueue.poll(2L,TimeUnit.SECONDS);

            if(null == result || result.equalsIgnoreCase("")){
                FLAG = false;
                System.out.println(Thread.currentThread().getName() + "\t 超过2s没有取到蛋糕,消费退出");
                return;
            }

            System.out.println(Thread.currentThread().getName()+"\t 消费蛋糕队列" + result + "成功");
        }
    }

    public void stop(){
        this.FLAG = false;
    }

}


public class ProdConsumer_BlockQueueDemo {
    public static void main(String[] args) {
        MyResoure myResoure = new MyResoure(new ArrayBlockingQueue<>(10));

        new Thread(() -> {
            System.out.println(Thread.currentThread().getName()+"\t 生产线程启动");
            try {
                // 生产
                myResoure.myProd();
            } catch (Exception e) {
                e.printStackTrace();
            }
        },"Prod").start();



        new Thread(() -> {
            System.out.println(Thread.currentThread().getName()+"\t 生产线程启动");
            try {
                // 消费
                myResoure.myConsumer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        },"Consumer").start();


        // 暂停5s
        try {TimeUnit.SECONDS.sleep(5);} catch (Exception e) {e.printStackTrace();}
        System.out.println("5s时间到,老板叫停,活动结束");
        myResoure.stop();
    }
}
