package cn.myfreecloud.java.blockqueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BlockingQueueDemo04 {
    public static void main(String[] args) throws Exception {
        BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(3);

        System.out.println(blockingQueue.offer("a", 2L, TimeUnit.SECONDS));
        System.out.println(blockingQueue.offer("a", 2L, TimeUnit.SECONDS));
        System.out.println(blockingQueue.offer("a", 2L, TimeUnit.SECONDS));


        // 不够了就阻塞2s
        System.out.println(blockingQueue.offer("a", 2L, TimeUnit.SECONDS));
    }
}
