package cn.myfreecloud.java.volatileuse;

/**
 * 经典的单例模式的演示 - 懒汉模式
 */
public class ClassicSingleton {
    private static ClassicSingleton instance = null;

    private ClassicSingleton() {
        // 单机版单线程下只会被打印一次,被构造一次
        System.out.println(Thread.currentThread().getName() + "\t 我是构造方法 ClassicSingleton()");
    }


    // synchronized 太重了,严重影响服务器性能,我们应该采用DCL模式 Double Check Lock 双端检索机制
    // 双端检索机制就是在加锁的前后都进行一次判断
    public static ClassicSingleton getInstance() {
        // 卫生间没人了,才进去
        if (instance == null) {

            // 使用同步代码块的方式来保证线程安全  // 给单间上锁
            synchronized (ClassicSingleton.class){
                // 加锁之前和加锁之后都进行一次判断 // 推门测试上锁
                if (instance == null) {
                    instance = new ClassicSingleton();
                }
            }
        }
        return instance;
    }

    public static void main(String[] args) {
        // 单线程 main线程的操作
//        System.out.println(ClassicSingleton.getInstance() == ClassicSingleton.getInstance());
//        System.out.println(ClassicSingleton.getInstance() == ClassicSingleton.getInstance());
//        System.out.println(ClassicSingleton.getInstance() == ClassicSingleton.getInstance());

        // 并发多线程之后会出问题 单例模式失效

        for (int i = 1; i < 40; i++) {
            new Thread(() -> {
                ClassicSingleton.getInstance();
            }, String.valueOf(i)).start();
        }

        //1	 我是构造方法 ClassicSingleton()
        //6	 我是构造方法 ClassicSingleton()
        //5	 我是构造方法 ClassicSingleton()
        //4	 我是构造方法 ClassicSingleton()
        //3	 我是构造方法 ClassicSingleton()
        //2	 我是构造方法 ClassicSingleton()
    }
}

