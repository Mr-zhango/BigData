package cn.myfreecloud.java.volatiletest;

public class T1 {
    volatile int n = 0;
    public void add(){
        n++;
    }
}
