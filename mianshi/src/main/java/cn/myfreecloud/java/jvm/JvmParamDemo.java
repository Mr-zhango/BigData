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
 *      -Xmx4096m
 *      -Xss1024K
 *      -XX:MetaSpaceSize=512M
 *      -XX:+PrintCommandLineFlags
 *      -XX:+PrintGCDetails
 *      -XX:+UseSerialGC
 *
 *
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
