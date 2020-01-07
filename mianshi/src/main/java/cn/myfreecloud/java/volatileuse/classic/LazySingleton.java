package cn.myfreecloud.java.volatileuse.classic;


/**
 * java 单例模式的 懒汉模式 需要时在创建
 */
public class LazySingleton {

    //1.将构造方式私有化，不允许外边直接创建对象
    private LazySingleton() {
    }

    //2.声明类的唯一实例，使用private static修饰
    private static LazySingleton instance;

    //3.提供一个用于获取实例的方法，使用public static修饰
    public static LazySingleton getInstance() {
        if (instance == null) {
            instance = new LazySingleton();
        }
        return instance;
    }
}