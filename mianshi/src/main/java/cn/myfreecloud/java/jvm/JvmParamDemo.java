package cn.myfreecloud.java.jvm;


/**
 * jvm标准参数:
 *      -version
 *      -help
 *      java -showversion
 *
 *  X参数:
 *   -Xint 解释执行
 *   -Xcomp 第一次使用就编译成本地代码
 *   -Xmixed 混合模式
 *
 *  --参数:
 *      Boolean类型
 *          -XX:
 *              +表示开启
 *              -表示关闭
 *      kv设置值类型
 *
 *      jinfo举例
 *
 *
 *      查看jvm初始化参数命令
 *      java -XX:+PrintFlagsInitial = 默认参数
 *      查询修改过的参数值
 *      java -XX:+PrintFlagsFinal  := 用户修改过的值
 *
 *
 *      jvm常用参数配置
 *      -Xms128m
 *          最小堆，jvm运行的默认堆大小(物理内存的1/64)
 *      -Xmx4096m
 *          指定最大堆，即堆内存的上线，当实际内存接近上线时会发生GC。(物理内存的1/4) 等价于 -XX:MaxHeapSize
 *      -Xss1024K
 *          为jvm启动的每个线程分配的内存大小 等价于-XX:ThreadStackSize(默认128k-512k)
 *      -Xmn
 *          设置新生代的大小
 *      -XX:MetaSpaceSize=512M
 *          元数据空间大小
 *      -XX:+PrintCommandLineFlags
 *
 *      -XX:+PrintGCDetails
 *          打印GC信息
 *      -XX:+UseSerialGC
 *
 *  常用参数设置
 *  -Xms128m -Xmx4096m -Xss1024k -XX:MetaSpaceSize=512m -XX:+PrintCommandLineFlags -XX:+PrintGCDetails -XX:+UseSerialGC
 *
 *
 *
 */
public class JvmParamDemo {
    public static void main(String[] args) throws Exception {

        System.out.println("***********hello gc");
        Thread.sleep(Integer.MAX_VALUE);


    }
}
