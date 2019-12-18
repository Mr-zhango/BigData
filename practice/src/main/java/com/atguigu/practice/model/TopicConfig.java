package com.atguigu.practice.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by didi on 17/10/20.
 */
public class TopicConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicConfig.class);

    private final String topic;

    private final int partitions;

    private final String consumerPropsFile;

    private final ProcessorConf[] processorConfs;

    public TopicConfig(String topic, ProcessorConf[] processorConfs, int partitions, String consumerPropsFile) {
        this.topic = topic;
        this.processorConfs = processorConfs;
        this.partitions = partitions;
        this.consumerPropsFile = consumerPropsFile;
    }

    public final String getTopic() {
        return topic;
    }

    public final ProcessorConf[] getProcessorConfs() {
        return processorConfs;
    }

    public final int getPartitions() {
        return partitions;
    }

    public final String getConsumerPropsFile() {
        return consumerPropsFile;
    }


    public List<Processor> getStandardProcessors() {
        List<Processor> ret = null;
        for(ProcessorConf processor : processorConfs) {
            try{
                Processor processor1 = new Processor(processor.getTransformer(), processor.getOperator(), processor.getTimeDimensions());
                if(ret == null) {
                    ret = new ArrayList<>();
                }
                ret.add(processor1);
            } catch(ServiceException e) {
                LOGGER.error(ErrorCode.ERRORCODE_TRANSFORMER_CLASS_NOT_FOUND.getMsg(), e);
            }
        }
        return ret;
    }

    public static class ProcessorConf {
        private String transformer;

        private String[] timeDimensions;

        private String operator;

        public ProcessorConf(String transformer, String operator, String[] timeDimensions) {
            this.transformer = transformer;
            this.timeDimensions = timeDimensions;
            this.operator = operator;

        }

        public String getTransformer() {
            return transformer;
        }

        public String[] getTimeDimensions() {
            return timeDimensions;
        }

        public String getOperator() {
            return operator;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            String sep = "";
            if(timeDimensions != null) {
                for(String timeDimension : timeDimensions) {
                    sb.append(sep);
                    sb.append(timeDimension);
                    sep = ",";
                }
            }
            sb.append("]");
            return "Processor:{transformer:" + transformer
                    + ", timeDimensions:" + sb.toString()
                    + ", operator:" + operator + "}";
        }

    }

    @Override
    public String toString() {
        StringBuilder processorsSb = new StringBuilder();
        processorsSb.append("[");
        String sep = "";
        if(processorConfs != null) {
            for(ProcessorConf processor : processorConfs) {
                processorsSb.append(sep);
                processorsSb.append(processor.toString());
                sep = ",";
            }
        }
        processorsSb.append("]");

        return "TopicConfig:{topic:" + topic
                + ", partitions:" + partitions
                + ", processors:" + processorsSb.toString()
                + ", consumerPropsFile:" + consumerPropsFile + "}";
    }
}
