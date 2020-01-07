package cn.myfreecloud.java.volatileuse.classic;

/**
 * java 单例模式的 饿汉模式 直接创建实例对象
 */
public class ESingleton {

    //1.将构造方法私有化，不允许外部直接创建对象
    private ESingleton() {

    }

    // 饿汉模式 直接创建实例对象 体现
    //2.创建类的唯一实例，使用private static修饰
    private static ESingleton instance = new ESingleton();

    //3.提供一个用于获取实例的方法，使用public static修饰
    public static ESingleton getInstance() {
        return instance;
    }
}