package cn.myfreecloud.java.ref;

import java.lang.ref.SoftReference;

/**
 * 软引用:内存够用就保留,不够用就回收!
 */
public class SoftRef {

    public static void SoftRef_Memory_Enough() {
        Object object1 = new Object();
        SoftReference<Object> object2 = new SoftReference<>(object1);



    }

    public static void SoftRef_Memory_NotEnough() {

    }

    public static void main(String[] args) {

    }
}
