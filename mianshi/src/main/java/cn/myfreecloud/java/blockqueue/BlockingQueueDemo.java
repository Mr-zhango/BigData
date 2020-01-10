package cn.myfreecloud.java.blockqueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * 阻塞队列,排队购票,先到先得
 * <p>
 * 当阻塞队列是空时,从队列中获取元素的操作会被阻塞
 * 当阻塞队列是满时,往队列里添加元素的操作将会被阻塞
 * <p>
 * ArrayBlockingQueue:由数组结构组成的有界阻塞队列
 * LinkedBlockingQueue:由链表结构组成的有界(但大小默认值为Integer.NAX_VALUE)阻塞队列
 * PriorityBlockingQueue:支持优先级排序的无界阻塞队列
 * DelayQueue:使用优先级实现的延迟无界阻塞队列
 * SynchronousQueue:不存储元素的阻塞队列,也即单个元素的队列
 * LinkedTransferQueue:由链表结构组成的无界阻塞队列
 * LinkedBlockingDeque:由链表结构组成的双向阻塞队列
 */

public class BlockingQueueDemo {
    public static void main(String[] args) {
        BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(3);

        // 插入
        System.out.println(blockingQueue.add("a"));
        System.out.println(blockingQueue.add("b"));
        System.out.println(blockingQueue.add("c"));

        // 只能存3个,多放报错
        // System.out.println(blockingQueue.add("d"));

        System.out.println("**********************************");
        // 检查排在队首的一个是谁
        System.out.println(blockingQueue.element());

        // 移除
        System.out.println(blockingQueue.remove());
        System.out.println(blockingQueue.remove());
        System.out.println(blockingQueue.remove());

    }
}
