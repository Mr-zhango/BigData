package cn.myfreecloud.java.volatileuse.classic;

public class TestSingleton {
    public static void main(String[] args) {
        //饿汉模式
        ESingleton s1 = ESingleton.getInstance();
        ESingleton s2 = ESingleton.getInstance();
        if (s1 == s2) {
            System.out.println("s1和s2是同一个实例");
        } else {
            System.out.println("s1和s2不是同一个实例");
        }

        //懒汉模式
        LazySingleton s3 = LazySingleton.getInstance();
        LazySingleton s4 = LazySingleton.getInstance();
        if (s3 == s4) {
            System.out.println("s3和s4是同一个实例");
        } else {
            System.out.println("S3和s4不是同一个实例");
        }

    }
}