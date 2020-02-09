package cn.myfreecloud.java.ref;

public class StrongRef {
    public static void main(String[] args) {
        Object object1 = new Object();
        Object obj2 = object1;
        object1 = null;
        System.gc();

        System.out.println(obj2);

    }
}
