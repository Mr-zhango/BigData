package cn.myfreecloud.java.ref;

public class StrongRef {
    public static void main(String[] args) {
        // 创建新的对象
        Object object1 = new Object();
        // obj2强引用 object1 ,垃圾回收之后obj2不会被回收
        Object obj2 = object1;
        object1 = null;
        System.gc();

        System.out.println(obj2);

    }
}
