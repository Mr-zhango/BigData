package cn.myfreecloud.java.lock.readwritelock;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// 资源类
class MyCache {

    // 放入缓存的东西一般使用 volatile 修饰 需要保证可见性 (不保证原子性,禁止指令重排)
    private volatile Map<String,Object> map = new HashMap<>();

    // 可重入的读写锁
    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * 写操作,必须 原子+独占 ,整个过程必须是一个完整的统一体,中间不允许被分割,被打断
     * @param key
     * @param value
     */
    public void put(String key,Object value){

        rwLock.writeLock().lock();
        try {
            System.out.println(Thread.currentThread().getName() + "\t 正在写入:" +key);

            // 暂停0.3s 用来模拟网络拥堵的效果
            try {TimeUnit.MICROSECONDS.sleep(300);} catch (Exception e) {e.printStackTrace();}

            map.put(key,value);
            System.out.println(Thread.currentThread().getName() + "\t 写入完成:");

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            rwLock.writeLock().unlock();
        }
    }

    public void get(String key){

        rwLock.readLock().lock();
        try {
            System.out.println(Thread.currentThread().getName() + "\t 正在读取:");
            // 暂停0.3s 用来模拟网络拥堵的效果
            try {TimeUnit.MICROSECONDS.sleep(300);} catch (Exception e) {e.printStackTrace();}
            Object result = map.get(key);
            System.out.println(Thread.currentThread().getName() + "\t 读取完成:" + result);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            rwLock.readLock().unlock();
        }
    }

    // 清除缓存
    public void clearMap(){

    }
}

public class ReadWriteLockDemo01 {
    public static void main(String[] args) {

        MyCache myCache = new MyCache();


        // 写数据
        for (int i = 1; i <= 5; i++) {
            final int tempInt = i ;

            new Thread(
                    () -> {
                        myCache.put(tempInt + "" , +tempInt + "");
                    }, String.valueOf(i)).start();
        }

        // 读数据
        for (int i = 1; i <= 5; i++) {
            final int tempInt = i ;

            new Thread(
                    () -> {
                        myCache.get(tempInt +"");
                    }, String.valueOf(i)).start();
        }

        //1	 正在写入:1
        //2	 正在写入:2
        //3	 正在写入:3
        //4	 正在写入:4
        //5	 正在写入:5
        //1	 正在读取:1
        //2	 正在读取:2
        //3	 正在读取:3
        //4	 正在读取:4
        //5	 正在读取:5
        //4	 写入完成:
        //2	 写入完成:
        //3	 写入完成:
        //1	 写入完成:
        //5	 写入完成:
        //5	 读取完成:5
        //1	 读取完成:1
        //3	 读取完成:3
        //4	 读取完成:null
        //2	 读取完成:2

    }
}
