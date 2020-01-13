package cn.myfreecloud.java.callable;


import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;


class MyThread implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        System.out.println("**********************come in callable");
        return 1024;
    }
}

public class CallableDemo {
    public static void main(String[] args) throws Exception {

        // 现在有两个线程

        FutureTask<Integer> futureTask = new FutureTask<>(new MyThread());

        Thread t1 = new Thread(futureTask,"AA");

        t1.start();
        int result01 = 100;

        // 要求获得Callable线程的计算结果,如果没有计算完成就要去强求,会导致堵塞,值需要计算完成
        int result02 = futureTask.get();  // 建议放在最后,或者使用自旋的方式来进行判断


        System.out.println("*************result:"+ (result01 + result02));
    }
}
