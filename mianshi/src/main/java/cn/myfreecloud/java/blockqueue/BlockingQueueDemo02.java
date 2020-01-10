package cn.myfreecloud.java.blockqueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BlockingQueueDemo02 {
    public static void main(String[] args) {
        BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(3);

        System.out.println(blockingQueue.offer("a"));
        System.out.println(blockingQueue.offer("a"));
        System.out.println(blockingQueue.offer("a"));
        // 失败了false 不抛异常
        System.out.println(blockingQueue.offer("a"));
        // 取出队列的顶端元素
        System.out.println(blockingQueue.peek());


        // 取出元素
        System.out.println(blockingQueue.poll());
        System.out.println(blockingQueue.poll());
        System.out.println(blockingQueue.poll());
        // 没有就是null
        System.out.println(blockingQueue.poll());

    }
}
