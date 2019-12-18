package com.atguigu.practice.model;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeInterval implements Comparable<TimeInterval>{

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeInterval.class);

    private Interval interval;

    public TimeInterval(DateTime start, DateTime end, Interval bound) throws ServiceException{

        if(bound != null && !bound.contains(new Interval(start, end))) {
            LOGGER.error(ErrorCode.ERRORCODE_TIMEINTERVAL_OUT_OF_BOUND.getMsg() + ", start:" + start + ", end:" + end + ", bound:" + bound.toString());
            throw new ServiceException("bound:" + bound.toString(), ErrorCode.ERRORCODE_TIMEINTERVAL_OUT_OF_BOUND);
        }
        interval = new Interval(start, end);
    }

    public int compareTo(TimeInterval timeInterval) {
        return getStart().compareTo(timeInterval.getStart());
    }

    public DateTime getStart() {
        if(interval == null) {
            return null;
        }
        return interval.getStart();
    }

    public DateTime getEnd() {
        if(interval == null) {
            return null;
        }
        return interval.getEnd();
    }

    public TimeInterval createTimeInterval(String str) throws ServiceException {
        return null;
    }

    public String toString() {
        return "TimeInterval:{start:" + getStart().toString() +
                ", end:" + getEnd().toString() + "}";
    }

}
