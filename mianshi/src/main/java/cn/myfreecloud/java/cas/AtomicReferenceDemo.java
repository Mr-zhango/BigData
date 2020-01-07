package cn.myfreecloud.java.cas;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 原子引用问题
 * ABA:实质就是狸猫换太子
 */

@Getter
@ToString
@AllArgsConstructor
class User {
    String userName;
    int age;
}


public class AtomicReferenceDemo {
    public static void main(String[] args) {

        // 利用原子引用来原子一个对象
        User zhangsan = new User("zhangsan",22);
        User lisi = new User("lisi",25);

        AtomicReference<User> atomicReference= new AtomicReference<>();
        atomicReference.set(zhangsan);
        System.out.println(atomicReference.compareAndSet(zhangsan, lisi) + "\t "+atomicReference.get().toString());
    }
}
