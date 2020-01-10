package cn.myfreecloud.java.blockqueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * 阻塞型的队列,满了之后一直阻塞,直到队列有空间了
 */
public class BlockingQueueDemo03 {
    public static void main(String[] args) throws Exception {
        BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(3);

        blockingQueue.put("a");
        blockingQueue.put("b");
        blockingQueue.put("c");

        System.out.println("===================================================");
        // blockingQueue.put("d");


        blockingQueue.take();
        blockingQueue.take();
        blockingQueue.take();

        // 必须消费才能结束,否则线程一直在等待
        blockingQueue.take();


    }
}
