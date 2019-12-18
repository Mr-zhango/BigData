package com.atguigu.practice.model;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class QueryTimePeriodCollection {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTimePeriodCollection.class);

    private Set<String> timePeriods;
    private List<TimeInterval> timeIntervals;
    private Map<DateTime, Set<String>> invertedIndexMap;

    public QueryTimePeriodCollection() {

        timePeriods = new HashSet<>();
        timeIntervals = new ArrayList<>();
        invertedIndexMap = new HashMap<>();
    }

    public Set<String> getTimePeriods() {
        return timePeriods;
    }

    public List<TimeInterval> getTimeIntervals() {
        return timeIntervals;
    }

    public Map<DateTime, Set<String>> getInvertedIndexMap() {
        return invertedIndexMap;
    }

    public void add(String timePeriod, TimeInterval timeInterval, TimeDimension timeDimension) throws ServiceException{

        timePeriods.add(timePeriod);
        timeIntervals.add(timeInterval);
        switch(timeDimension) {
            case DAILY:
                for(DateTime datetime = timeInterval.getStart(); datetime.isBefore(timeInterval.getEnd()); datetime = datetime.plusDays(1)) {
                    if(!invertedIndexMap.containsKey(datetime)) {
                        Set<String> set = new HashSet<>();
                        invertedIndexMap.put(datetime, set);
                    }
                    invertedIndexMap.get(datetime).add(timePeriod);
                }
                break;
            /**
             * server to Heat-Map
             */
            case FIVE_MINUTELY:
                for(DateTime datetime = timeInterval.getStart(); datetime.isBefore(timeInterval.getEnd()); datetime = datetime.plusMinutes(5)) {
                    if(!invertedIndexMap.containsKey(datetime)) {
                        Set<String> set = new HashSet<>();
                        invertedIndexMap.put(datetime, set);
                    }
                    invertedIndexMap.get(datetime).add(timePeriod);
                }
                break;
            /**
             * server to City-Area
             */
            case MINUTELY:
                for (DateTime dateTime = timeInterval.getStart(); dateTime.isBefore(timeInterval.getEnd()); dateTime = dateTime.plusMinutes(1)){
                    if (!invertedIndexMap.containsKey(dateTime)){
                        Set<String> set = new HashSet<>();
                        invertedIndexMap.put(dateTime, set);
                    }
                    invertedIndexMap.get(dateTime).add(timePeriod);
                }
                break;
        }

    }

    public void mergeIntervals() {
        Collections.sort(timeIntervals);

        if(timeIntervals == null || timeIntervals.isEmpty()) {
            return;
        }
        List<TimeInterval> ret = new ArrayList<>();

        TimeInterval currentTimeInterval = null;

        for(TimeInterval timeInterval : timeIntervals) {
            if(timeInterval != null) {
                if(currentTimeInterval == null || timeInterval.getStart().isAfter(currentTimeInterval.getEnd())) {
                    if(currentTimeInterval != null) {
                        ret.add(currentTimeInterval);
                    }
                    currentTimeInterval = timeInterval;
                } else {
                    if(timeInterval.getEnd().isAfter(currentTimeInterval.getEnd())){
                        try {
                            currentTimeInterval = new TimeInterval(currentTimeInterval.getStart(), timeInterval.getEnd(), null);
                        } catch (Exception e) {
                            LOGGER.error(ErrorCode.ERRORCODE_TIMEINTERVAL_OUT_OF_BOUND.getMsg() + ", start:" + currentTimeInterval.getStart() + ", end:" + timeInterval.getEnd());

                        }
                    }
                }

            }
        }
        ret.add(currentTimeInterval);

        timeIntervals = ret;

    }
}