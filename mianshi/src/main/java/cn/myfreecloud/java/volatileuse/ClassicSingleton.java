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

    public static synchronized ClassicSingleton getInstance() {
        if (instance == null) {
            instance = new ClassicSingleton();
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

