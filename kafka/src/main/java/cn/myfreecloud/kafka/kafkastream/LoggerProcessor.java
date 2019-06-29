package cn.myfreecloud.kafka.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author: zhangyang
 * @date: 2019/6/29 17:34
 * @description:
 */
public class LoggerProcessor implements Processor<byte[],byte[]> {

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
        String line = new String(bytes2);
        //去除">>>"
        String s = line.replaceAll(">>>", "");

        byte[] bytesResult = s.getBytes();

        context.forward(bytes,bytesResult);
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
