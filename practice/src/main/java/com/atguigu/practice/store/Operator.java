package com.atguigu.practice.store;

import com.atguigu.practice.utils.Utils;
import com.atguigu.practice.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class Operator {
    private static Logger LOGGER = LoggerFactory.getLogger(Operator.class);

    /**
     * This method takes
     *      1. a valid event containing (topic, key, metrics, timestamp),
     *          where valid event means (topic, key, metrics, timestamp) are non-empty and legal
     *      2. a boolean parameter async,
     *          which specifies whether the update operation is asynchronous (i.e wait and then batch write to store) or synchronous(do NOT wait and write to store instantly)
     *      3. a list of TimeDimension, which indicates the granularities to store counter metrics, i.e. increment in a 1-day cell, or increment in a 5-min cell
     * as parameters.
     *
     * Increment in the store based on what is specified in event, boolean async and list of time dimensions.
     *
     * @param event
     * @param async
     * @param timeDimensions
     * @return updateResponse
     * @throws ServiceException
     */
    public abstract void update(Event event, boolean async, List<TimeDimension> timeDimensions, UpdateResponse updateResponse) throws ServiceException;


    /**
     * This method takes
     *      1.a valid Query containing (key, metrics, timestamp, timePeriods),
     *      where valid Query means (key, metrics, timestamp, timePeriods) are non-empty and legal
     * as a parameter
     *
     * Query the store to get what is specified in the Query
     *
     * @param query
     * @return queryResponse
     * @throws com.atguigu.practice.model.ServiceException
     */
    public abstract void query(Query query, QueryResponse queryResponse) throws ServiceException;

    public abstract boolean isValidEvent(Event event, UpdateResponse updateResponse);

    public abstract boolean isValidQuery(Query query, QueryResponse queryResponse);
    /**
     * This method is a helper function for query(Query query)
     * which takes
     *      1.a key to query
     *      2.a queryTimePeriodCollection which is a wrapper class containing helpful information of the timePeriods to query
     *      3.metrics set to query
     *      4.a QueryResponse to store the query result
     * as parameters
     *
     * @param key
     * @param timePeriodCollection
     * @param counterMetrics
     * @param statusMetrics
     * @param queryResponse
     * @throws ServiceException
     */
    protected abstract void queryMetrics(String key, QueryTimePeriodCollection timePeriodCollection, Set<String> counterMetrics, Set<String> statusMetrics, QueryResponse queryResponse) throws ServiceException;

    /**
     * This method cleans up and closes the operator connector
     */
    public abstract void close();


    /**
     * This method takes a String timePeriod and parse it to TimeInterval, which is a wrapper class of joda DateTime Interval.
     * It throws ServiceException when the parse fails.
     *
     * @param timePeriod
     * @return
     * @throws ServiceException
     */
    protected abstract TimeInterval createTimeInterval(String timePeriod) throws ServiceException;

    /**
     * This method takes a list of metrics and filters out duplicate or unsupported metrics
     * and returns a set of supported metrics as well as log error status to queryResponse
     * ! If the metrics is null or empty, we will fill it with all supported metrics
     *
     * @param metrics
     * @param supportedMetrics
     * @param queryResponse
     * @param numInvalid
     */
    protected void getValidMetrics(List<String> metrics, Set<String> supportedMetrics, QueryResponse queryResponse, IntWrapper numInvalid, Set<String> counterMetrics, Set<String> statusMetrics) {

        if(supportedMetrics == null) {
            return;
        }

        if(metrics == null) {
            metrics = new ArrayList<>();
        }
        if(metrics.isEmpty()) {
            metrics.addAll(supportedMetrics);
        }

        for(String metric : metrics) {
            if(!supportedMetrics.contains(metric)){
                numInvalid.i++;
                queryResponse.error.put("[INVALID_METRIC] metric:" + metric, ErrorCode.ERRORCODE_INVALID_QUERY_METRIC_NOT_SUPPORTED.getMsg());
                LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY_METRIC_NOT_SUPPORTED.getMsg() + ", metric:" + metric);
            } else if (Utils.isCounter(metric)) {
                counterMetrics.add(metric);
            } else {
                statusMetrics.add(metric);
            }
        }
    }

    /**
     * This method builds QueryTimePeriodCollection from a list of string timePeriods and log error status to queryResponse
     *
     * @param timePeriods
     * @param timeDimension
     * @param queryResponse
     * @param numInvalid
     * @return
     */
    protected QueryTimePeriodCollection getQueryTimePeriodCollection(List<String> timePeriods, TimeDimension timeDimension, QueryResponse queryResponse, IntWrapper numInvalid) {

        if(timePeriods == null || timePeriods.isEmpty()) {
            return null;
        }

        QueryTimePeriodCollection timePeriodCollection = new QueryTimePeriodCollection();

        for(String timePeriod : timePeriods) {
            try{
                TimeInterval timeInterval = createTimeInterval(timePeriod);
                timePeriodCollection.add(timePeriod, timeInterval, timeDimension);
            } catch (ServiceException e) {
                numInvalid.i++;
                queryResponse.error.put("[INVALID_TIMEPERIOD] tp:" + timePeriod, e.getErrorMsg());
                LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY_TIMEPERIOD_NOT_SUPPORTED.getMsg() + ", tp:" + timePeriod, e);
            }

        }

        return timePeriodCollection;
    }

    protected void logFailStatus(Set<String> counterMetrics, Set<String> statusMetrics, IntWrapper numInvalidMetrics, QueryTimePeriodCollection timePeriodCollection, IntWrapper numInvalidTimePeriods, QueryResponse queryResponse) {

        int sizeOfCounterMetrics = counterMetrics.size();
        int sizeOfStatusMetrics = statusMetrics.size();

        if(sizeOfStatusMetrics == 0 && sizeOfCounterMetrics == 0 ) {
            queryResponse.status = Status.FAIL.getCode();
        } else if (sizeOfCounterMetrics > 0 && sizeOfStatusMetrics == 0) {
            if(timePeriodCollection == null || timePeriodCollection.getTimePeriods().isEmpty()){
                queryResponse.status = Status.FAIL.getCode();
                if(timePeriodCollection == null) {
                    queryResponse.error.put("[MISSING_TP]", ErrorCode.ERRORCODE_INVALID_QUERY_MISSING_PARAMETER.getMsg() + ", parameter tp is missing");
                }
            } else if(numInvalidMetrics.i > 0 || numInvalidTimePeriods.i > 0) {
                queryResponse.status = Status.PARTIALLY_FAIL.getCode();
            }
        } else if (sizeOfCounterMetrics == 0 && sizeOfStatusMetrics > 0) {
            if(numInvalidMetrics.i > 0) {
                queryResponse.status = Status.PARTIALLY_FAIL.getCode();
            }
        } else {
            if(timePeriodCollection == null || timePeriodCollection.getTimePeriods().isEmpty()) {
                queryResponse.status = Status.PARTIALLY_FAIL.getCode();
                if(timePeriodCollection == null) {
                    queryResponse.error.put("[MISSING_TP]", ErrorCode.ERRORCODE_INVALID_QUERY_MISSING_PARAMETER.getMsg() + ", parameter tp is missing");
                }
            } else if (numInvalidMetrics.i > 0 || numInvalidTimePeriods.i > 0) {
                queryResponse.status = Status.PARTIALLY_FAIL.getCode();
            }
        }
    }

    protected static class IntWrapper {
        public int i = 0;
    }

}
