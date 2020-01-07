package cn.myfreecloud.java.volatiletest;

public class RestoreSortDemo {


    int a = 0;
    boolean flag = false;

    public void method01() {
        a = 1;
        flag = true;

    }

    // 多线程环境中线程交替执行,由于编译器编译过程中和代码在内存中指令运行过程中有优化重排的存在
    // 导致多个线程中使用的变量能否保证一致性是无法确定的,导致最终的结果无法预测
    // 添加 volatile 关键字修饰变量能够禁止指令重排
    public void method02() {
        if (flag) {
            a = a + 5;
            System.out.println("resultValue:" + a);
        }
    }


}
