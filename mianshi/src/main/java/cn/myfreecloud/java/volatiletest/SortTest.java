package cn.myfreecloud.java.volatiletest;

public class SortTest {
    public static void main(String[] args) {

        // 加了volatile就会禁止指令重排,保证多线程下代码的执行顺序是一致的
       mySort();
    }
    public static void mySort() {
        int x = 11; // 1
        int y = 12; // 2

        x = x + 5; // 3
        y = x * x; // 4

        // 编译器编译之后的顺序可能是
        // 1234
        // 2134
        // 1324
        // 这就是指令重排
        System.out.println(x);
        System.out.println(y);

    }
}
