package cn.myfreecloud.java.lock;

import java.util.concurrent.CountDownLatch;

public class CountDownLatchDemo {
    public static void main(String[] args) throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(6);

        for (int i = 1; i <= 6 ; i++) {
            new Thread(() ->{
                System.out.println(Thread.currentThread().getName() + "\t 国家,被灭");
                // 做减法
                countDownLatch.countDown();
            },CountryEnum.forEach_CountryEnum(i).getRetMessage()).start();
        }

        // 一直等到减到0了才能向下执行
        countDownLatch.await();
        System.out.println(Thread.currentThread().getName() + "\t *******秦统一天下");
        System.out.println(CountryEnum.ONE);
        System.out.println(CountryEnum.ONE.getRetCode());
        System.out.println(CountryEnum.ONE.getRetMessage());
    }

    private static void closeDoor() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(6);


        for (int i = 1; i <= 6 ; i++) {
            new Thread(() ->{
                System.out.println(Thread.currentThread().getName() + "\t 上完晚自习,离开教室");
                // 做减法
                countDownLatch.countDown();
            },String.valueOf(i)).start();
        }

        // 一直等到减到0了才能向下执行
        countDownLatch.await();
        System.out.println(Thread.currentThread().getName() + "\t 最后一个人关门上锁");
    }
}
