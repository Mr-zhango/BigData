package com.atguigu.practice.model;

import com.atguigu.practice.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KVEntry {
    private static final Logger LOGGER = LoggerFactory.getLogger(KVEntry.class);

    private final String key;
    private final List<HEntry> hEntries;

    public KVEntry(String key, List<HEntry> hEntries) {
        this.key = key;
        this.hEntries = hEntries;
    }

    public String getKey() {
        return key;
    }

    public List<HEntry> getHEntries() {
        return hEntries;
    }

    public static KVEntry fromCounter(Counter counter) {
        if(counter == null) {
            return null;
        }
        DateTime dt = new DateTime(counter.getTimestamp());
        String month = Utils.format2Digit(dt.getMonthOfYear());
        String key = makeKey(counter.getKey(), month);
        //logger.error("month, day, hour, min: " + month + " " + day + " " + dt.getHourOfDay() + " " + dt.getMinuteOfHour());
        List<HEntry> hEntries = new ArrayList<HEntry>();
        for (Map.Entry<String, Number> entry : counter.getMetrics().entrySet()) {
            for (TimeDimension timeDim : counter.getTimeDimensions()) {
                String hKey = makeHKey(timeDim.getSCode(), dt, entry.getKey());
                if(hKey != null) {
                    HEntry hEntry = new HEntry(hKey, entry.getValue(), true);
                    hEntries.add(hEntry);
                } else {
                    LOGGER.error(ErrorCode.ERRORCODE_INVALID_TIMEDIMENSION.getMsg() + ", timeDimension:" + timeDim);
                }
            }
        }

        if(StringUtils.isNotBlank(key) && !hEntries.isEmpty()) {
            return new KVEntry(key, hEntries);
        }

        return null;
    }


    private static String makeKey(String key, String month) {
        return key + "_" + month;
    }

    private static String makeHKey(String timeDim, DateTime dt, String metric) {
        TimeDimension timeDimension = TimeDimension.getFromSCode(timeDim);
        String minute = Utils.format2Digit(dt.getMinuteOfHour());
        String hour = Utils.format2Digit(dt.getHourOfDay());
        String day = Utils.format2Digit(dt.getDayOfMonth());

        if(timeDimension != null) {
            switch(timeDimension) {
                case MINUTELY:
                    return timeDim + day + hour + minute + "_" + metric;

                case HOURLY:
                    return timeDim + day + hour + "_" + metric;

                case DAILY:
                    return timeDim + day + "_" + metric;

                case MONTHLY:
                    return timeDim + "_" + metric;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "KVEntry:{key:" + key +
        ", hEntries:[" + StringUtils.join(hEntries, ",")+ "]}";
    }



}
