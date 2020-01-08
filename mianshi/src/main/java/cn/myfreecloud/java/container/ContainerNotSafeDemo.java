package cn.myfreecloud.java.container;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

public class ContainerNotSafeDemo {
    public static void main(String[] args) {

        // "18" java.util.ConcurrentModificationException
        // new HashSet<>();   线程不安全
        // 解决:
        //  1.Collections.synchronizedSet(new HashSet<>());
        //  2.new CopyOnWriteArraySet<>();
        //
        //  CopyOnWriteArraySet<>();   底层实现
        //  public CopyOnWriteArraySet() {
        //        al = new CopyOnWriteArrayList<E>();
        //    }
        //  3.new ConcurrentHashMap<>();



        Map hashMap = new ConcurrentHashMap<>();
        for (int i = 1; i <= 30; i++) {
            new Thread(() -> {
                hashMap.put(Thread.currentThread().getName(),UUID.randomUUID().toString().substring(0,8));
                System.out.println(Thread.currentThread().getName()+hashMap);
            }, String.valueOf(i)).start();
        }


    }

    private static void containerListNotSafe() {
        /**
         * [1722b832]
         * [1722b832, 5d6b6709]
         * [1722b832, 5d6b6709, a762d84e]
         */
        //  new Vector<>();
        //  Collections.synchronizedList(new ArrayList<>());
        //  new CopyOnWriteArrayList<>();
        List<String> list = new CopyOnWriteArrayList<>();
        // 开启了20个线程
        for (int i = 1; i <= 30; i++) {
            new Thread(() -> {
               list.add(UUID.randomUUID().toString().substring(0,8));
                System.out.println(Thread.currentThread().getName()+list);
            }, String.valueOf(i)).start();
        }

        // java.util.ConcurrentModificationException 并发修改异常
        //1.故障现象
        //	java.util.ConcurrentModificationException 并发修改异常
        //2.导致原因
        //
        //3.解决方案
        //	3.1 new Vector<>();
        //	3.2 Collections.synchronizedList(new ArrayList<>());
        //	3.3 new CopyOnWriteArrayList<>();
        //4.优化方案
        //

        // 写时复制
        // CopyOnWrite,容器即写时复制的容器.往一个容器添加元素的时候,不直接往当前容器Object[]添加,而是先将当前容器Object[]进行copy,复制出
        // 一个新的容器,Object[] newElements,然后新的容器Object[] newElements里面添加元素,添加完元素之后,
        // 再将原容器的引用指向新的容器 setArray(newElements);这样做的好处是可以对CopyOnWrite容器进行并发的读,而不需要加锁,
        // 因为当前容器不会添加任何元素.所以CopyOnWrite容器也是一种读写分离的思想,

        // /**
        //     * Appends the specified element to the end of this list.
        //     *
        //     * @param e element to be appended to this list
        //     * @return {@code true} (as specified by {@link Collection#add})
        //     */
        //    public boolean add(E e) {
        //        final ReentrantLock lock = this.lock;
        //        lock.lock();
        //        try {
        //            Object[] elements = getArray();
        //            int len = elements.length;
        //            Object[] newElements = Arrays.copyOf(elements, len + 1);
        //            newElements[len] = e;
        //            setArray(newElements);
        //            return true;
        //        } finally {
        //            lock.unlock();
        //        }
        //    }


    }


}
