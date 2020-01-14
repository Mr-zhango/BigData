package cn.myfreecloud.java.lock;


import java.util.concurrent.TimeUnit;

class HoldLockThread implements Runnable{

    private String lockA;
    private String lockB;

    public HoldLockThread(String lockA, String lockB) {
        this.lockA = lockA;
        this.lockB = lockB;
    }

    @Override
    public void run() {
        synchronized (lockA){
            System.out.println(Thread.currentThread().getName()+"\t 自己持有:"+lockA+"\t 尝试获得:"+lockB);
            // 暂停2s
            try {TimeUnit.SECONDS.sleep(2);} catch (Exception e) {e.printStackTrace();}

            synchronized (lockB){
                System.out.println(Thread.currentThread().getName()+"\t 自己持有:"+lockB+"\t 尝试获得:"+lockA);
                // 暂停2s
                try {TimeUnit.SECONDS.sleep(2);} catch (Exception e) {e.printStackTrace();}
            }
        }
    }
}
public class DeadLockDemo {
    public static void main(String[] args) {
        String lockA = "lockA";
        String lockB = "lockB";

        new Thread(new HoldLockThread(lockA,lockB),"thread AAA").start();
        new Thread(new HoldLockThread(lockB,lockA),"thread BBB").start();
    }
}
