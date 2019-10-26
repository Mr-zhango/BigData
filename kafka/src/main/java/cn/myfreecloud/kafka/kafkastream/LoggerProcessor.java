package cn.myfreecloud.kafka.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author: zhangyang
 * @date: 2019/6/29 17:34
 * @description:
 */
public class LoggerProcessor implements Processor<byte[],byte[]> {

    // 上下文对象
    private ProcessorContext context;

    /**
     * 初始化方法
     * @param processorContext
     */
    @Override
    public void init(ProcessorContext processorContext) {
        context = processorContext;
    }

    /**
     * 实时处理单词带有">>>"前缀的内容。例如输入"atguigu>>>ximenqing"，最终处理成"ximenqing"
     * @param bytes
     * @param bytes2
     */
    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        //获取一行数据
        // 1. 获取数据
        String line = new String(bytes2);
        //去除">>>"
        // 2. 处理数据
        /*String s = line.replaceAll(">>>", "");

        byte[] bytesResult = s.getBytes();*/

        if(line.contains(">>>")){
            String[] split = line.split(">>>");
            String trim = split[1].trim();
            // 输出数据
            context.forward(bytes,trim.getBytes());
        }else{
            // 不包含的话就原样输出
            context.forward(bytes,bytes2);
        }
    }

    /**
     * 处理与实践戳相关的
     * @param l
     */
    @Override
    public void punctuate(long l) {

    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {

    }
}
