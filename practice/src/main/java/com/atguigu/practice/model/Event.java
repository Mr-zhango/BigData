package com.atguigu.practice.model;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Event {

    private static final Logger LOGGER = LoggerFactory.getLogger(Event.class);

    private static final String TOPIC = "topic";
    private static final String KEY = "key";
    private static final String METRICS = "metrics";
    private static final String TIMESTAMP = "ts";

    private final String topic;
    private final String key;
    private final Map<String, Number> metrics;
    private final long timestamp;
    private final boolean isCounter;
    private final Type type;

    public Event(String topic, String key, long timestamp, Map<String, Number> metrics, boolean isCounter, Type type) {
        super();
        this.topic = topic;
        this.key = key;
        this.metrics = metrics;
        this.timestamp = timestamp;
        this.isCounter = isCounter;
        this.type = type;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public Map<String, Number> getMetrics() {
        return metrics;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isCounter() {
        return isCounter;
    }

    public Type getType() {
        return type;
    }

    public static Event fromRequest(HttpServletRequest request) {
        String requestURI = request.getRequestURI();
        String topic = request.getParameter(TOPIC);
        String key = request.getParameter(KEY);
        String metricsStr = request.getParameter(METRICS);
        String timestampStr = request.getParameter(TIMESTAMP);

        String[] requestURIArr = requestURI.split("/");
        String typeStr = requestURIArr[requestURIArr.length - 1];
        Type type = Type.getType(typeStr);

        long timestamp = DateTime.now().getMillis();
        if(timestampStr != null){

            try{
                timestamp = Long.valueOf(timestampStr);
            } catch(NumberFormatException nfe) {
                LOGGER.error(ErrorCode.ERRORCODE_FAIL_TO_CAST_STRING_TO_LONG.getMsg(), nfe);
            }
        }

        List<String> metricNames;
        Map<String, Number> metrics = null;
        if(metricsStr != null) {
            metricNames = Arrays.asList(metricsStr.split("\\s*,\\s*"));
            metrics = new HashMap<>();
            for (String name : metricNames) {
                metrics.put(name, Integer.valueOf(1));
            }
        }

        return new Event(topic, key, timestamp, metrics, true, type);
    }

    @Override
    public String toString() {
        return "Event:{topic:" + topic +
                ", key:" + key +
                ", metrics:" + metrics +
                ", timestamp:" + String.valueOf(timestamp) +
                ", isCounter:" + isCounter +
                ", type:" + type.getlCode() + "}";
    }
}
