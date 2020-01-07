package cn.myfreecloud.java.cas;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * cas:
 * 比较并交换 compareAndSet()
 * 相当于git的提交,只有最新版本的才能提交成功
 *
 * cas底层原理:
 *  自旋锁
 *  Unsafe类,相当于C/C++中的指针,根据内存偏移地址 直接操作内存
 *  cas的全称是Compare-And-Swap 他是一条CPU并发原语,功能就是判断内存中某个位置的值是否为预期的值,如果是则更改为新的值,这个过程是原子的.
 *  CAS并发原语体现在JAVA语言中就是sun.misc.Unsafe类中的各个方法,存在于rt.jar中,程序运行时就会加载,调用UnSafe类中的CAS方法,JVM会帮我们实现出CAS汇编指令,
 *  这是一种完全依赖于硬件的功能,通过它实现了原子操作,再次强调,由于CAS是一种系统原语,原语属于操作系统用语范畴,是由若干条指令组成的,用户完成某一个功能的一个过程,并且原语的执行必须是连续的,
 *  再 执行过程中不允许被中断,也就是说CAS是一条CPU的原子指令,不会造成所谓的数据不一致问题
 *
 *  为什么使用cas,而不使用 synchronized ????
 *  synchronized加锁同一时间段只允许有一个线程来访问一致性得到了保证,但是并发性能下降,
 *  cas是通过数据和他修改后的地址偏移量来进行比较,使用while循环反复比较直到比较成功为止,既保证了一致性,也提高了并发性
 *
 *  cas优缺点
 *  缺点:底层实现自旋锁,需要多次比较,循环时间长,开销大如果CAS失败,会一直进行尝试,如果CAS长时间不成功,可能会给CPU带来很大的开销
 */
public class CASDemo {
    public static void main(String[] args) {

        AtomicInteger atomicInteger = new AtomicInteger(5);

        // main do thing......

        // 2019
        System.out.println(atomicInteger.compareAndSet(5, 2019) + "\t current value:" + atomicInteger.get());
        // 2019
        System.out.println(atomicInteger.compareAndSet(5, 2020) + "\t current value:" + atomicInteger.get());

        //unsafe.getAndAddInt(this, valueOffset, 1);
        int andIncrement = atomicInteger.getAndIncrement();
        System.out.println(andIncrement);
    }
}
