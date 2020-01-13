package cn.myfreecloud.java.thread;


import java.util.concurrent.*;

/**
 * 线程池的7大参数
 *
 *      1.corePoolSize:线程池中的常驻核心数 (今日当值柜员)
 *      2.maximumPoolSize:线程能够容纳的同时执行的最大线程数,此值大于等于1  (网点窗口物理上限)
 *      3.keepAliveTime:多余线程的存活时间  (空闲的线程数,超过corePoolSize的线程数)
 *           当前线程池数量超过corePoolSize时,当空闲时间达到keepAliveTime时,多余空闲线程会被销毁,直到剩下corePoolSize个线程为止
 *           默认情况下只有当线程池中的线程数大于corePoolSize时,keepAliveTime才会起作用,直到线程池中的线程数不大于corePoolSize
 *      4.unit:keepAliveTime的单位
 *      5.workQueue: 任务队列,被提交但是尚未被执行的任务(侯客队列)
 *      6.threadFactory 表示生成线程池中工作线程的线程工厂,用于创建线程,一般用默认的即可
 *      7.handler:拒绝策略,表示当队列满了,并且工作线程大于线程池的最大线程数(maximumPoolSize)时,如何来拒绝执行请求的runnable策略
 *
 */


// 手写线程池
public class MyThreadPoolDemo {
    public static void main(String[] args) {
        // 自定义的线程池
        ExecutorService threadPool = new ThreadPoolExecutor(
                2,5,
                1L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(3),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardPolicy());

        // 超过就报异常
        // new ThreadPoolExecutor.AbortPolicy()

        //
        // ThreadPoolExecutor.CallerRunsPolicy()
        try {

            // AbortPolicy
            // 5 完全能承受
            // 8 最大承受数
            // 9 就要报异常  java.util.concurrent.RejectedExecutionException: Task cn.myfreecloud.java.thread.MyThreadPoolDemo$$Lambda$1/551734240@39aeed2f rejected from java.util.concurrent.ThreadPoolExecutor@6767c1fc[Running, pool size = 5, active threads = 5, queued tasks = 0, completed tasks = 3]

            // CallerRunsPolicy() 回退调用者
            // main	 办理业务
            // main	 办理业务

            // DiscardOldestPolicy()
            // 丢去等待时间最长的任务

            // DiscardPolicy()
            // 直接丢弃任务



            for (int i = 1; i <=10 ; i++) {
                // execute() 方法,叫下一个客户
                threadPool.execute(()->{
                    System.out.println(Thread.currentThread().getName() + "\t 办理业务");
                });

                // 暂停200ms
                // try {TimeUnit.MILLISECONDS.sleep(200);} catch (Exception e) {e.printStackTrace();}
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdown();
        }



    }
}
