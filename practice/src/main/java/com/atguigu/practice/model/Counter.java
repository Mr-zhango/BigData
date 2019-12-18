package com.atguigu.practice.model;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by linghongbo on 2016/3/22.
 */
public class Counter {

    private final String key;
    private final List<TimeDimension> timeDimensions;
    private final long timestamp;
    private final Map<String, Number> metrics;

    public Counter(String key, List<TimeDimension> timeDimensions, long timestamp, Map<String, Number> metrics) {

        this.key = key;
        this.timeDimensions = timeDimensions;
        this.timestamp = timestamp;
        this.metrics = metrics;
    }

    public String getKey() {
        return key;
    }

    public List<TimeDimension> getTimeDimensions() {
        return timeDimensions;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, Number> getMetrics() {
        return metrics;
    }

    public static Counter fromEvent(Event event, List<TimeDimension> timeDimensions) {
        String key = event.getKey();
        Type type = event.getType();
        if(Type.PASSENGER.equals(type) || Type.GS_DRIVER.equals(type) || Type.TAXI_DRIVER.equals(type)) {
            key = type.getsCode() + key;
        }
        long timestamp = event.getTimestamp();
        Map<String, Number> metrics = event.getMetrics();

        return new Counter(key, timeDimensions, timestamp, metrics);
    }

    @Override
    public String toString() {

        return "Counter:{key:" + key +
                ", timeDimensions:[" + StringUtils.join(timeDimensions, ",") +
                "], metrics:{" + metrics  +
                "}, timestamp:" + String.valueOf(timestamp) + "}";
    }
}
