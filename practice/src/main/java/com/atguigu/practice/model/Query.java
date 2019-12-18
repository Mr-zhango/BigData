package com.atguigu.practice.model;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;


public class Query {
    private static final Logger LOGGER = LoggerFactory.getLogger(Query.class);

    private static final String KEY = "key";
    private static final String METRICS = "metrics";
    private static final String TIMEPERIODS = "tp";
    private static final String CALLER = "caller";
    private static final String APP = "app";

    private final String key;
    private final List<String> metrics;
    private final List<String> timePeriods;
    private final Type type;
    private final String caller;
    private final App app;

    public Query(String key, List<String> metrics, List<String> timePeriods, Type type, String caller, App app) {
        this.key = key;
        this.metrics = metrics;
        this.timePeriods = timePeriods;
        this.type = type;
        this.caller = caller;
        this.app = app;
    }


    public String getKey() {
        return key;
    }

    public List<String> getMetrics() {
        return metrics;
    }

    public List<String> getTimePeriods() {
        return timePeriods;
    }

    public Type getType() {
        return type;
    }

    public String getCaller() {
        return caller;
    }

    public App getApp() {
        return app;
    }

    public static Query fromRequest(HttpServletRequest request) {
        String caller = request.getParameter(CALLER);
        String requestURI = request.getRequestURI();
        String key = request.getParameter(KEY);
        String metricsStr = request.getParameter(METRICS);
        String timePeriodsStr = request.getParameter(TIMEPERIODS);
        String appName = request.getParameter(APP);

        String[] requestURIArr = requestURI.split("/");
        String typeStr = requestURIArr[requestURIArr.length - 1];
        Type type = Type.getType(typeStr);
        App app = App.getApp(appName);

        List<String> metrics = null;
        List<String> timePeriods = null;
        if(metricsStr != null) {
            metrics = Arrays.asList(metricsStr.split("\\s*,\\s*"));
        }
        if(timePeriodsStr != null) {
            timePeriods = Arrays.asList(timePeriodsStr.split("\\s*,\\s*"));
        }

        return new Query(key, metrics, timePeriods, type, caller, app);
    }


    @Override
    public String toString() {
        return "Query:{key:" + String.valueOf(key) +
                ", metrics:[" + StringUtils.join(metrics, ",") +
                "], timePeriods:[" + StringUtils.join(timePeriods, ",") +
                "], type:" + type.getlCode() + "}";
    }
}



 /*long timestamp = DateTime.now().getMillis();
        String timestampStr = request.getParameter(TIMESTAMP);
        if(timestampStr != null){
            try{
                String dateRegex = "\\d{4}\\-\\d{2}\\-\\d{2}";
                String dateTimeRegex = "\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}";
                String millisRegex = "\\d+";
                Matcher dateRegexMatcher = Pattern.compile(dateRegex).matcher(timestampStr);
                Matcher dateTimeRegexMatcher = Pattern.compile(dateTimeRegex).matcher(timestampStr);
                Matcher millisRegexMatcher = Pattern.compile(millisRegex).matcher(timestampStr);

                DateTimeFormatter formatter;
                DateTime dateTime;
                if(dateRegexMatcher.matches()) {
                    formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
                    dateTime = formatter.parseDateTime(timestampStr);
                    timestamp = dateTime.getMillis();
                } else if(dateTimeRegexMatcher.matches()) {
                    formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
                    dateTime = formatter.parseDateTime(timestampStr);
                    timestamp = dateTime.getMillis();
                } else if(millisRegexMatcher.matches()) {
                    timestamp = Long.valueOf(timestampStr);
                }

            } catch(NumberFormatException nfe) {
                LOGGER.error(ErrorCode.ERRORCODE_FAIL_TO_CAST_STRING_TO_LONG.getMsg(), nfe);
            }
        }*/

