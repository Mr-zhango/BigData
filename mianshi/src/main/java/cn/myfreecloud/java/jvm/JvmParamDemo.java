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
 *      -XX:SurvivorRation
 *          它定义了新生代中Eden区域和Survivor区域（From幸存区或To幸存区）的比例，默认为8，也就是说Eden占新生代的8/10，From幸存区和To幸存区各占新生代的1/10
 *      -XX:NewRation
 *          老年代：新生代=4，即old：(Eden + Survivor from + Survivor to) ，则说明新生代为整个堆区的1/5
 *          ps：XX:NewRation和Xmn的区别：都使用来指定新生代的大小，但区别在于Xmn是固定的，二XX:NewRation则是一个比值，即会随着堆区的内存的大小的变化而变化
 *       -XX:MaxTenuringThreshold
 *          设置垃圾的最大年龄 默认是15,需要经过15次垃圾回收,从新生带去到养老区,必须在0-15之间
 *
 *
 *  常用参数设置
 *  -Xms128m -Xmx4096m -Xss1024k -XX:MetaSpaceSize=512m -XX:+PrintCommandLineFlags -XX:+PrintGCDetails -XX:+UseSerialGC
 *
 *  -XX:+DisableExplicitGC
 *  -XX:G1ReservePercent=25
 *  -XX:GCLogFileSize=104857600
 *  -XX:+HeapDumpOnOutOfMemoryError
 *  -XX:HeapDumpPath=/data/apps/app_name/logs/java.hprof
 *  -XX:InitialHeapSize=268435456
 *  -XX:InitiatingHeapOccupancyPercent=40
 *  -XX:MaxDirectMemorySize=1073741824
 *  -XX:MaxGCPauseMillis=200
 *  -XX:MaxHeapSize=268435456
 *  -XX:NumberOfGCLogFiles=10
 *  -XX:-OmitStackTraceInFastThrow
 *  -XX:+PrintCommandLineFlags
 *  -XX:+PrintGC
 *  -XX:+PrintGCDateStamps
 *  -XX:+PrintGCTimeStamps
 *  -XX:ThreadStackSize=256
 *  -XX:+UseCompressedClassPointers
 *  -XX:+UseCompressedOops
 *  -XX:+UseG1GC
 *  -XX:+UseGCLogFileRotation
 *
 */
public class JvmParamDemo {
    public static void main(String[] args) throws Exception {

        System.out.println("***********hello gc");
        Thread.sleep(Integer.MAX_VALUE);


    }
}
