package com.atguigu.practice.utils;

import com.atguigu.practice.model.TimeDimension;
import com.atguigu.practice.store.QDBOperator;
import org.joda.time.DateTime;

import java.util.*;

public class QDBUtils {


    public static String makeKey(String key, String month) {
        return key + "_"  + month;
    }


    public static String makeHKeyDaily(String metric, String day) {
        return TimeDimension.DAILY.getSCode() + day + "_" + metric;
    }

    public static List<String> makeFields(DateTime dt, String timeDim, String metric) {
        List<String> fields = new ArrayList<String>();
        fields.add(makeHKey(timeDim, dt, metric));
        return fields;
    }

    public static String makeHKey(String timeDim, DateTime dt, String metric) {
        TimeDimension timeDimension = TimeDimension.getFromSCode(timeDim);
        String minute = Utils.format2Digit(dt.getMinuteOfHour());
        String hour = Utils.format2Digit(dt.getHourOfDay());
        String day = Utils.format2Digit(dt.getDayOfMonth());

        if (timeDimension != null) {
            switch (timeDimension) {
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

    public static Map<String, QDBOperator.HKeysClass> genKeyFields(String key, Set<DateTime> queryDays, Set<String> counterMetrics, Set<String> statusMetrics) {

        Map<String, QDBOperator.HKeysClass> keyedFields = new HashMap<>();
        String queryField;
        String queryKey;

        if(queryDays != null && counterMetrics != null) {
            for (DateTime queryDay : queryDays) {
                //long start = System.nanoTime();

                String month = Utils.format2Digit(queryDay.getMonthOfYear());
                String day = Utils.format2Digit(queryDay.getDayOfMonth());

                queryKey = makeKey(key, month);

                for(String metric : counterMetrics) {
                    queryField = makeHKeyDaily(metric, day);
                    // LOGGER.error("[QUERY]: gen key and fields used=" + (System.nanoTime() - start) + "ns");

                    //start = System.nanoTime();
                    if (!keyedFields.containsKey(queryKey)) {
                        keyedFields.put(queryKey, new QDBOperator.HKeysClass());
                    }
                    keyedFields.get(queryKey).hKeys.add(queryField);
                    keyedFields.get(queryKey).dateTimes.add(queryDay);
                    keyedFields.get(queryKey).metrics.add(metric);

                    //LOGGER.error("[QUERY]: insert into keyedFields used=" + (System.nanoTime() - start) + "ns");
                }
            }
        }

        /**
         * status metrics
         */
        if(statusMetrics != null) {
            for(String metric : statusMetrics) {
                if(!Utils.isCounter(metric)){
                    // LOGGER.error("[QUERY]: gen key and fields used=" + (System.nanoTime() - start) + "ns");

                    //start = System.nanoTime();
                    if (!keyedFields.containsKey(key)) {
                        keyedFields.put(key, new QDBOperator.HKeysClass());
                    }
                    keyedFields.get(key).hKeys.add(metric);
                    keyedFields.get(key).metrics.add(metric);
                }
                //LOGGER.error("[QUERY]: insert into keyedFields used=" + (System.nanoTime() - start) + "ns");
            }
        }



        return keyedFields;
    }
}
