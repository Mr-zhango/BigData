package com.atguigu.practice.kafka;

import com.atguigu.practice.meta.ConfManager;
import com.atguigu.practice.model.*;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class PracticeConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PracticeConsumer.class);

    private final ConsumerConnector consumerConnector;

    private final String topic;

    private List<Processor> processors;

    private ExecutorService executor;

    public PracticeConsumer(String taskName, String consumerPropsFile) throws ServiceException{

        Properties properties = new Properties();

        try {
            properties.load(PracticeConsumer.class.getClassLoader().getResourceAsStream(consumerPropsFile));
        } catch (IOException ioe) {
            LOGGER.error(ErrorCode.ERRORCODE_LOAD_CONSUMER_PROPS_FAIL.getMsg(), ioe);
            throw new ServiceException(ErrorCode.ERRORCODE_LOAD_CONSUMER_PROPS_FAIL);
        }

        ConsumerConfig config = new ConsumerConfig(properties);//创建kafka消费端配置信息
        consumerConnector = Consumer.createJavaConsumerConnector(config);//创建消费端链接器


        Map<String, TopicConfig> topicConfigMap = ConfManager.getInstance().topicConfigMap;
        TopicConfig topicConfig = topicConfigMap.get(taskName);

        if(topicConfig == null) {
            LOGGER.error(ErrorCode.ERRORCODE_NULL_TOPIC_CONFIG.getMsg());
            throw new ServiceException(ErrorCode.ERRORCODE_INVALID_TOPIC_CONFIG);
        } else {
            this.topic = topicConfig.getTopic();
            processors = topicConfig.getStandardProcessors();
            if(processors == null){
                LOGGER.error(ErrorCode.ERRORCODE_NULL_PROCESSOR.getMsg());
                throw new ServiceException(ErrorCode.ERRORCODE_INVALID_TOPIC_CONFIG);

            }
        }

    }

    public void start(int a_numThreads) {
        LOGGER.error("Start Kafka consumer server");

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(Collections.singletonMap(topic, new Integer(a_numThreads)));
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(a_numThreads);
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            ConsumeKafkaThread thread = new ConsumeKafkaThread(stream, threadNumber);
            executor.submit(thread);
            LOGGER.error("Kafka thread num: "+ threadNumber+" starts up");

            threadNumber++;
        }
    }
    public void stop() {
        LOGGER.error("Stop Kafka consumer server");
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(100000, TimeUnit.MILLISECONDS)) {
                    LOGGER.error("Timed out after " + 100000 + "ms waiting for consumer threads to shut down, exiting uncleanly");
                }
            } catch (InterruptedException e) {
                LOGGER.error(ErrorCode.ERRORCODE_SHUTDOWN_INTERRUPTED.getMsg(), e);
            }
        }
        /**
         * In QDB operator case, sync the pipelines so that remaining events are written to QDB, and then close jedises
         */
        for(Processor processor : processors) {
            processor.getOperator().close();
            //WriterProcessor.getInstance().close(processor.getOperator());
        }
        //ConfManager.close();
    }

    public class ConsumeKafkaThread implements Runnable {
        private KafkaStream stream;
        private int threadNumber;

        public ConsumeKafkaThread(KafkaStream stream, int threadNumber) {
            this.threadNumber = threadNumber;
            this.stream = stream;
        }
        public void run() {
            try {
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                Long readNum = 0l;
                long b = System.currentTimeMillis();
                while (it.hasNext()){

                    String message = new String(it.next().message());
                    handleMessage(message);
                    readNum++;
                    if(readNum % 10000 == 0){
                        long e = System.currentTimeMillis();
                        LOGGER.error("----printEntry: " + readNum + " threadNum: " + threadNumber + " time: " + e + " avg uses:" + ((e-b)/(double)readNum) + "ms------");
                        readNum = 0l;
                        b = System.currentTimeMillis();
                    }
                }
                LOGGER.error("Shutting down Thread: " + threadNumber);
            } catch (Throwable t) {
                LOGGER.error(ErrorCode.ERRORCODE_EXCEPTION.getMsg(), t);
            }


        }

        private void handleMessage(String message) {

            for(Processor processor : processors) {
                /**
                 * Parse message
                 */
                List<Event> events = null;
                try {
                    events = processor.getTransformer().transform(message);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("transform error", e);
                }

                if(events != null) {

                    for(Event event : events) {
                    /**
                     * Write to store
                     */
                        UpdateResponse updateResponse = new UpdateResponse();

                        try {
                            processor.getOperator().update(event, true, processor.getTimeDimensions(), updateResponse);

                        } catch (Exception e) {
                            updateResponse.status = Status.FAIL.getCode();
                            updateResponse.error.put(ErrorCode.ERRORCODE_WRITE_FAIL.getMsg(), e.getMessage());
                            LOGGER.error(ErrorCode.ERRORCODE_WRITE_FAIL.getMsg(), e);
                        }
                    }
                }
            }
        }
    }


    public static void main(String[] args) {
        final ConfManager conf = ConfManager.getInstance();
        conf.init();
        Map<String, TopicConfig> topicConfigMap = conf.topicConfigMap;
        // 定义一个List用于存放多个PracticeConsumer消费者类
        List<PracticeConsumer> consumerList = null;
        // 遍历ConfManager解析得到的topic信息类
        for(Map.Entry<String, TopicConfig> entry : topicConfigMap.entrySet()) {
            String taskName = entry.getKey();
            TopicConfig topicConfig = entry.getValue();
            String topic = topicConfig.getTopic();
            int numThreads = topicConfig.getPartitions() == 0 ? 10 : topicConfig.getPartitions();

            List<Processor> processors = topicConfig.getStandardProcessors();

            if(topic != null && topicConfig != null && processors != null) {
                LOGGER.error("topic:" + topic + ", numThreads:" + numThreads);

                for(Processor processor : processors) {
                    List<String> timeDimensions = new ArrayList<>();
                    for(TimeDimension timeDimension : processor.getTimeDimensions()) {
                        timeDimensions.add(timeDimension.getSCode());
                    }
                    LOGGER.error("{transformer:" + processor.getTransformer().getClass().getName() + ", operator:" + processor.getOperator().getClass().getName() + ", timeDimensions:" + StringUtils.join(timeDimensions, ",") + "}");
                }
                try{
                    final PracticeConsumer consumer = new PracticeConsumer(taskName, topicConfig.getConsumerPropsFile());
                    if(consumerList == null) {
                        consumerList = new ArrayList<>();
                    }
                    consumerList.add(consumer);
                    consumer.start(numThreads);

                } catch(Throwable t) {
                    LOGGER.error(ErrorCode.ERRORCODE_EXCEPTION.getMsg(), t);
                }
            }

        }

        final List<PracticeConsumer> finalConsumerList = consumerList;
        Runtime.getRuntime().addShutdownHook( new Thread() {
            public void run() {
                try {
                    if (finalConsumerList != null) {
                        for (PracticeConsumer consumer : finalConsumerList) {
                            LOGGER.error("## stop TorrentConsumer key:" + consumer.topic);
                            consumer.stop();
                        }
                    }
                } catch (Throwable e) {
                    LOGGER.error("## something goes wrong when stopping TorrentConsumer:\n{}"+ ExceptionUtils.getFullStackTrace(e));
                } finally {
                    LOGGER.error("## canal client is down.");
                }
            }
        });

    }
}
