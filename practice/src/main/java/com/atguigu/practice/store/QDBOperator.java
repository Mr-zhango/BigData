package com.atguigu.practice.store;

import com.atguigu.practice.utils.Const;
import com.atguigu.practice.utils.QDBUtils;
import com.atguigu.practice.utils.Utils;
import com.atguigu.practice.factory.JedisFactory;
import com.atguigu.practice.meta.ConfManager;
import com.atguigu.practice.model.*;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

public class QDBOperator extends Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(QDBOperator.class);

    private Map<Integer, Jedis> jedisMap; //jedisMap池子

    private Map<Integer, Integer> resourceDistMap; //资源字典池用于存放rand随机数

    private Map<Integer, Pipeline> pipelineMap; //pipeline字典，threadId为key

    private Map<Integer, List<FlatKVEntry>> commandsMap; //命令map，threadId为key

    private Map<Integer, Long> beginMap; // 用于存储threadId开始运行的时间

    private static Set<String> SUPPORTED_PASSENGER_METRICS; //乘客支持的指标metrics有哪些，如有新指标加入，需要更新client.pro配置文件

    private static Set<String> SUPPORTED_GS_DRIVER_METRICS;

    private static final Set<String> SUPPORTED_TAXI_DRIVER_METRICS;

    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    private final static int STORE_RETENTION = 30;

    private final static TimeDimension STORE_GRANULARITY = TimeDimension.DAILY; //日期维度，如果以天为维度redis hashmap中字段key为Dxx_metric


    static {
        SUPPORTED_PASSENGER_METRICS = ConfManager.serverConfig.getSupportedPassengerMetrics();

        SUPPORTED_GS_DRIVER_METRICS = ConfManager.serverConfig.getSupportedGsDriverMetrics();

        SUPPORTED_TAXI_DRIVER_METRICS = ConfManager.serverConfig.getSupportedTaxiDriverMetrics();
    }

    public QDBOperator() {
        jedisMap = new ConcurrentHashMap<>();
        resourceDistMap = new ConcurrentHashMap<>();
        pipelineMap = new ConcurrentHashMap<>();
        commandsMap = new ConcurrentHashMap<>();
        beginMap = new ConcurrentHashMap<>();
    }

    @Override
    public void update(Event event, boolean async, List<TimeDimension> timeDimensions, UpdateResponse updateResponse) throws ServiceException {
        if (!isValidEvent(event, updateResponse)) {
            LOGGER.error(ErrorCode.ERRORCODE_INVALID_EVENT.getMsg() + ": " + event.toString());
            return;
        }

        List<FlatKVEntry> flatKVEntryList;
        if (event.isCounter()) {
            Counter counter = Counter.fromEvent(event, timeDimensions);
            flatKVEntryList = FlatKVEntry.flattenKVEntry(KVEntry.fromCounter(counter));
        } else {
            flatKVEntryList = new ArrayList<>();
            for (Map.Entry<String, Number> entry : event.getMetrics().entrySet()) {
                String key = entry.getKey();
                Number value = entry.getValue();
                flatKVEntryList.add(new FlatKVEntry(event.getType().getsCode() + event.getKey(), new HEntry(key, value, false)));
            }
        }

        List<Integer> availablePools = new ArrayList<>();
        availablePools.add(0);
        availablePools.add(1);
        availablePools.add(2);
        pipeLine(flatKVEntryList, async, 0, availablePools, updateResponse);

    }

    @Override
    public void query(Query query, QueryResponse queryResponse) throws ServiceException {

        if (!isValidQuery(query, queryResponse)) {
            LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY.getMsg() + ", query:" + query.toString());
            return;
        }

        /**
         * Get parameters key, metrics, timePeriods from query
         */
        Type type = query.getType();
        String key = query.getKey();
        List<String> metrics = query.getMetrics();
        List<String> timePeriods = query.getTimePeriods();
        App app = query.getApp();

        /**
         * Filter out unsupported or duplicate metrics
         */
        IntWrapper numInvalidMetrics = new IntWrapper();
        Set<String> supported_metrics;
        if (Type.PASSENGER.equals(type)) {
            supported_metrics = SUPPORTED_PASSENGER_METRICS;
        } else if (Type.GS_DRIVER.equals(type)) {
            supported_metrics = SUPPORTED_GS_DRIVER_METRICS;
        } else {
            supported_metrics = SUPPORTED_TAXI_DRIVER_METRICS;
        }

        Set<String> counterMetrics = new HashSet<>();
        Set<String> statusMetrics = new HashSet<>();

        getValidMetrics(metrics, supported_metrics, queryResponse, numInvalidMetrics, counterMetrics, statusMetrics);

        /**
         * Construct a QueryTimePeriodCollection object from timePeriods
         * 1.Filter out unsupported or duplicate timePeriods
         * 2.Convert timePeriod String to a TimeInterval object
         * 3.Build invertedIndex for dateTime: dateTime -> timePeriod strings that contains the dateTime
         */
        IntWrapper numInvalidTimePeriods = new IntWrapper();

        QueryTimePeriodCollection timePeriodCollection = getQueryTimePeriodCollection(timePeriods, STORE_GRANULARITY, queryResponse,
                numInvalidTimePeriods);

        logFailStatus(counterMetrics, statusMetrics, numInvalidMetrics, timePeriodCollection, numInvalidTimePeriods, queryResponse);

        if (App.UBER.equals(app)) {
            returnStaticValuesForUberQuery(type.getsCode() + key, timePeriodCollection, counterMetrics, statusMetrics, queryResponse);
        } else {
            queryMetrics(type.getsCode() + key, timePeriodCollection, counterMetrics, statusMetrics, queryResponse);
        }

    }

    private void returnStaticValuesForUberQuery(String key, QueryTimePeriodCollection timePeriodCollection, Set<String> counterMetrics, Set<String>
            statusMetrics, QueryResponse queryResponse) throws ServiceException {
        Map<String, Object> metricValueMap = new HashMap<>();
        if (statusMetrics != null) {
            for (String metric : statusMetrics) {
                metricValueMap.put(metric, "uber");
            }
        }
        queryResponse.value = metricValueMap;
    }


    /**
     * Clean up and close the operator connector 1.sync the outstanding pipelines 2.return outstanding jedis to jedis pool
     */
    @Override
    public void close() {
        try {
            for (Map.Entry<Integer, Pipeline> entry : pipelineMap.entrySet()) {
                Pipeline pipeline = entry.getValue();
                if (pipeline != null) {
                    pipeline.sync();
                    LOGGER.error("sync pipeline before shutdown " + entry.getKey());
                }
            }
        } catch (Exception e) {
            LOGGER.error(ErrorCode.ERRORCODE_QDB_PIPE_SYNC_FAIL.getMsg(), e);
        }

        for (Map.Entry<Integer, Jedis> entry : jedisMap.entrySet()) {
            Jedis jedis = entry.getValue();
            if (jedis != null) {
                jedis.close();
                LOGGER.error("close jedis before shutdown " + entry.getKey());
            }
        }
    }

    /**
     * This method is a helper function for queryCounter(Query query) which takes a transformed key, a parsed queryTimePeriodCollection, valid metrics
     * set to query store and a QueryResponse to store the result
     */
    @Override
    protected void queryMetrics(String key, QueryTimePeriodCollection timePeriodCollection, Set<String> counterMetrics, Set<String> statusMetrics,
                                QueryResponse queryResponse) throws ServiceException {
        Map<String, Object> metricValueMap = new HashMap<>();

        Map<DateTime, Set<String>> invertedIndexMap = null;
        if (timePeriodCollection != null) {
            invertedIndexMap = timePeriodCollection.getInvertedIndexMap();
        }

        if ((counterMetrics == null || counterMetrics.isEmpty()) && (statusMetrics == null || statusMetrics.isEmpty())) {
            return;
        }

        Map<String, HKeysClass> keyedFields = QDBUtils.genKeyFields(key, invertedIndexMap == null ? null : invertedIndexMap.keySet(),
                counterMetrics, statusMetrics);


        if (keyedFields != null && keyedFields.size() > 0) {
            Jedis jedis = null;
            Random random = new Random();
            int rand = random.nextInt(3);

            long begin = System.currentTimeMillis();
            try {

                jedis = JedisFactory.getInstance().getJedisPool(rand).getResource();

                Pipeline pipeline = jedis.pipelined();

                List<redis.clients.jedis.Response<List<String>>> resultList = new ArrayList<>();
                for (Map.Entry<String, HKeysClass> entry : keyedFields.entrySet()) {
                    String queryKey = entry.getKey();
                    HKeysClass hKeysClass = entry.getValue();
                    List<String> hKeys = hKeysClass.hKeys;

                    redis.clients.jedis.Response<List<String>> result = pipeline.hmget(queryKey, hKeys.toArray(new String[0]));
                    resultList.add(result);
                }
                pipeline.sync();

                int i = 0;

                for (Map.Entry<String, HKeysClass> entry : keyedFields.entrySet()) {
                    HKeysClass hKeysClass = entry.getValue();
                    List<DateTime> dateTimes = hKeysClass.dateTimes;
                    List<String> metricList = hKeysClass.metrics;

                    List<String> partResult = resultList.get(i++).get();
                    int partResultSize = partResult.size();
                    for (int j = 0; j < partResultSize; j++) {
                        String pr = partResult.get(j);

                        if (pr != null) {

                            String metric = metricList.get(j);

                            if (Utils.isCounter(metric)) {
                                DateTime dt = dateTimes.get(j);

                                if (!metricValueMap.containsKey(metric)) {
                                    metricValueMap.put(metric, new HashMap<String, Double>());
                                }

                                Set<String> timePeriodSet = invertedIndexMap.get(dt);

                                Object object = metricValueMap.get(metric);

                                if (object instanceof Map) {
                                    Map map = (Map) object;

                                    for (String timePeriod : timePeriodSet) {

                                        if (!map.containsKey(timePeriod)) {
                                            map.put(timePeriod, Double.valueOf(pr));
                                        } else {
                                            map.put(timePeriod, (Double) map.get(timePeriod) + Double.valueOf(pr));
                                        }
                                    }
                                }
                            } else {
                                metricValueMap.put(metric, pr);
                            }

                        }
                    }

                }

                queryResponse.value = metricValueMap;
            } catch (Throwable e) {
                long end = System.currentTimeMillis();
                queryResponse.error.put(ErrorCode.ERRORCODE_QDB_READ_FAIL.getMsg(), "keyedFields:{" + keyedFields + "}");
                queryResponse.status = Status.FAIL.getCode();
                LOGGER.error(ErrorCode.ERRORCODE_QDB_READ_FAIL.getMsg() + ": keyedFields:{" + keyedFields + "}, " +
                        "timeout:" + (end - begin) + "ms", e);

                throw new ServiceException(ErrorCode.ERRORCODE_QDB_READ_FAIL);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }

        }

    }


    /**
     * This method create TimeInterval Object from a timePeriod string
     */
    @Override
    public TimeInterval createTimeInterval(String str) throws ServiceException {
        TimeInterval interval;
        if (StringUtils.isNotBlank(str)) {
            DateTime now = DateTime.now();
            str = str.toUpperCase();
            int dayOfWeek = now.getDayOfWeek();
            int dayOfMonth = now.getDayOfMonth();
            DateTime startOfToday = now.withTime(0, 0, 0, 0);
            DateTime startOfTomorrow = startOfToday.plusDays(1);
            DateTime startOfYesterday = startOfToday.minusDays(1);

            Interval limit = new Interval(now.minusDays(STORE_RETENTION).withTime(0, 0, 0, 0), now.plusDays(1).withTime(0, 0, 0, 0));

            switch (str) {
                case Const.TODAY:
                    interval = new TimeInterval(startOfToday, startOfTomorrow, limit);
                    break;
                case Const.YESTERDAY:
                    interval = new TimeInterval(startOfYesterday, startOfToday, limit);
                    break;
                case Const.THISWEEK:
                    interval = new TimeInterval(startOfToday.minusDays(dayOfWeek - 1), startOfTomorrow, limit);
                    break;
                case Const.LASTWEEK:
                    interval = new TimeInterval(startOfToday.minusDays(dayOfWeek + DateTimeConstants.DAYS_PER_WEEK - 1), startOfToday.minusDays
                            (dayOfWeek - 1), limit);
                    break;
                case Const.THISMONTH:
                    interval = new TimeInterval(startOfToday.minusDays(dayOfMonth - 1), startOfTomorrow, limit);
                    break;
                case Const.LASTMONTH:
                    int daysInLastMonth = now.minusMonths(1).dayOfMonth().getMaximumValue();
                    interval = new TimeInterval(startOfToday.minusDays(dayOfMonth + daysInLastMonth - 1), startOfToday.minusDays(dayOfMonth - 1),
                            limit);
                    break;
                default:
                    Matcher lastNDaysMatcher = Const.LASTNDAYS_PATTERN.matcher(str);
                    Matcher lastNDayMatcher = Const.LASTNDAY_PATTERN.matcher(str);
                    Matcher lastMDayToLastNDayMatcher = Const.LASTMDAYTOLASTNDAY_PATTERN.matcher(str);
                    Matcher yyyy_mm_ddToyyyy_mm_ddMatcher = Const.YYYY_MM_DDTOYYYY_MM_DD_PATTERN.matcher(str);
                    Matcher yyyy_mm_ddMatcher = Const.YYYY_MM_DD_PATTERN.matcher(str);
                    Matcher lastNWeeksMatcher = Const.LASTNWEEKS_PATTERN.matcher(str);
                    Matcher lastNWeekMatcher = Const.LASTNWEEK_PATTERN.matcher(str);
                    Matcher lastMWeekToLastNWeekMatcher = Const.LASTMWEEKTOLASTNWEEK_PATTERN.matcher(str);
                  /*  Matcher lastNMonthsMatcher = Const.LASTNMONTHS_PATTERN.matcher(str);
                    Matcher lastNMonthMatcher = Const.LASTNMONTH_PATTERN.matcher(str);
                    Matcher lastMMonthToLastNMonthMatcher = Const.LASTMMONTHTOLASTNMONTH_PATTERN.matcher(str);
*/
                    int m, n;

                    try {
                        if (lastNDaysMatcher.matches()) {
                            n = Integer.parseInt(lastNDaysMatcher.group(1));
                            interval = new TimeInterval(startOfToday.minusDays(n - 1), startOfTomorrow, limit);
                        } else if (lastNDayMatcher.matches()) {
                            n = Integer.parseInt(lastNDayMatcher.group(1));
                            interval = new TimeInterval(startOfToday.minusDays(n), startOfToday.minusDays(n - 1), limit);
                        } else if (lastMDayToLastNDayMatcher.matches()) {
                            m = Integer.parseInt(lastMDayToLastNDayMatcher.group(1));
                            n = Integer.parseInt(lastMDayToLastNDayMatcher.group(2));
                            if (m < n) {
                                int tmp = m;
                                m = n;
                                n = tmp;
                            }
                            interval = new TimeInterval(startOfToday.minusDays(m), startOfToday.minusDays(n - 1), limit);
                        } else if (yyyy_mm_ddToyyyy_mm_ddMatcher.matches()) {
                            String startDateStr = yyyy_mm_ddToyyyy_mm_ddMatcher.group(1);
                            String endDateStr = yyyy_mm_ddToyyyy_mm_ddMatcher.group(2);
                            DateTime startDateTime = FORMATTER.parseDateTime(startDateStr);
                            DateTime endDateTime = FORMATTER.parseDateTime(endDateStr);
                            //m = Days.daysBetween(startDateTime.toLocalDate(), dateTime.toLocalDate()).getDays();
                            //n = Days.daysBetween(endDateTime.toLocalDate(), dateTime.toLocalDate()).getDays();
                            interval = new TimeInterval(startDateTime, endDateTime.plusDays(1), limit);

                        } else if (yyyy_mm_ddMatcher.matches()) {
                            DateTime queryDateTime = FORMATTER.parseDateTime(str);
                            //m = Days.daysBetween(queryDateTime.toLocalDate(), dateTime.toLocalDate()).getDays();
                            //set(-m , -m);
                            interval = new TimeInterval(queryDateTime, queryDateTime.plusDays(1), limit);

                        } else if (lastNWeeksMatcher.matches()) {
                            n = Integer.parseInt(lastNWeeksMatcher.group(1));
                            interval = new TimeInterval(startOfToday.minusDays(dayOfWeek - 1).minusWeeks(n - 1), startOfTomorrow, limit);
                            //set(-dayOfWeek - (n - 1) * DateTimeConstants.DAYS_PER_WEEK + 1, 0);
                        } else if (lastNWeekMatcher.matches()) {
                            n = Integer.parseInt(lastNWeekMatcher.group(1));
                            interval = new TimeInterval(startOfToday.minusDays(dayOfWeek - 1).minusWeeks(n), startOfToday.minusDays(dayOfWeek - 1)
                                                                                                                         .minusWeeks(n - 1), limit);
                            // set(-dayOfWeek - n * DateTimeConstants.DAYS_PER_WEEK + 1, Math.min(0, -dayOfWeek - (n - 1) * DateTimeConstants
                            // .DAYS_PER_WEEK));
                        } else if (lastMWeekToLastNWeekMatcher.matches()) {
                            m = Integer.parseInt(lastMWeekToLastNWeekMatcher.group(1));
                            n = Integer.parseInt(lastMWeekToLastNWeekMatcher.group(2));
                            if (m < n) {
                                int tmp = m;
                                m = n;
                                n = tmp;
                            }
                            interval = new TimeInterval(startOfToday.minusDays(dayOfWeek - 1).minusWeeks(m), startOfToday.minusDays(dayOfWeek - 1)
                                                                                                                         .minusWeeks(n - 1), limit);
                            // set(-dayOfWeek - m * DateTimeConstants.DAYS_PER_WEEK + 1, Math.min(0, -dayOfWeek - (n - 1) * DateTimeConstants
                            // .DAYS_PER_WEEK));
                        }/* else if (lastNMonthsMatcher.matches()) {
                            n = Integer.parseInt(lastNMonthsMatcher.group(1));
                            DateTime startOfMonth = dateTime.minusMonths(n - 1).dayOfMonth().withMinimumValue();
                            set(-Days.daysBetween(startOfMonth.toLocalDate(), dateTime.toLocalDate()).getDays(), 0);
                        } else if (lastNMonthMatcher.matches()) {
                            n = Integer.parseInt(lastNMonthMatcher.group(1));
                            DateTime startOfLastNMonth = dateTime.minusMonths(n).dayOfMonth().withMinimumValue();
                            DateTime endOfLastNMonth = dateTime.minusMonths(n).dayOfMonth().withMaximumValue();
                            set(-Days.daysBetween(startOfLastNMonth.toLocalDate(), dateTime.toLocalDate()).getDays(),
                                    Math.min(0, -Days.daysBetween(endOfLastNMonth.toLocalDate(), dateTime.toLocalDate()).getDays()));


                        } else if(lastMMonthToLastNMonthMatcher.matches()) {
                            m = Integer.parseInt(lastMMonthToLastNMonthMatcher.group(1));
                            n = Integer.parseInt(lastMMonthToLastNMonthMatcher.group(2));
                            if(m < n){
                                int tmp = m;
                                m = n;
                                n = tmp;
                            }
                            DateTime startOfLastMMonth = dateTime.minusMonths(m).dayOfMonth().withMinimumValue();
                            DateTime endOfLastNMonth = dateTime.minusMonths(n).dayOfMonth().withMaximumValue();
                            set(-Days.daysBetween(startOfLastMMonth.toLocalDate(), dateTime.toLocalDate()).getDays(),
                                    Math.min(0, -Days.daysBetween(endOfLastNMonth.toLocalDate(), dateTime.toLocalDate()).getDays()));

                        }*/ else {
                            throw new ServiceException(ErrorCode.ERRORCODE_INVALID_QUERY_TIMEPERIOD_NOT_SUPPORTED);
                        }
                    } catch (NumberFormatException e) {
                        LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY_TIMEPERIOD_NOT_SUPPORTED.getMsg(), e);
                        throw new ServiceException(ErrorCode.ERRORCODE_INVALID_QUERY_TIMEPERIOD_NOT_SUPPORTED);
                    }
            }
        } else {
            throw new ServiceException(ErrorCode.ERRORCODE_INVALID_QUERY_TIMEPERIOD_NOT_SUPPORTED);
        }
        return interval;
    }

    @Override
    public boolean isValidEvent(Event event, UpdateResponse updateResponse) {
        if (event == null) {
            updateResponse.status = Status.FAIL.getCode();
            updateResponse.error.put("[NULL_EVENT]", ErrorCode.ERRORCODE_NULL_EVENT.getMsg());
            return false;
        }

        String key = event.getKey();
        Map<String, Number> metrics = event.getMetrics();
        Type type = event.getType();

        /**
         * Check if type is specified.
         */
        if (type == null) {
            updateResponse.status = Status.FAIL.getCode();
            updateResponse.error.put("[INVALID_EVENT_TYPE]", ErrorCode.ERRORCODE_INVALID_EVENT_TYPE.getMsg());
            LOGGER.error(ErrorCode.ERRORCODE_INVALID_EVENT_TYPE.getMsg());
            return false;
        }

        /**
         * Check if there are missing parameters in event
         */
        if (StringUtils.isBlank(key) || metrics == null || metrics.isEmpty()) {
            updateResponse.status = Status.FAIL.getCode();

            if (StringUtils.isBlank(key)) {
                updateResponse.error.put("[MISSING_KEY]", ErrorCode.ERRORCODE_INVALID_UPDATE_MISSING_PARAMETER.getMsg());
                LOGGER.error(ErrorCode.ERRORCODE_INVALID_UPDATE_MISSING_PARAMETER.getMsg() + ": key");
            }

            if (metrics == null || metrics.isEmpty()) {
                updateResponse.error.put("[MISSING_METRICS]", ErrorCode.ERRORCODE_INVALID_UPDATE_MISSING_PARAMETER.getMsg());
                LOGGER.error(ErrorCode.ERRORCODE_INVALID_UPDATE_MISSING_PARAMETER.getMsg() + ": metrics");
            }
            return false;
        }

        /**
         * check if key is in valid format.
         * */
        if (!Utils.isAllDigits(key)) {
            updateResponse.status = Status.FAIL.getCode();
            updateResponse.error.put("[INVALID_KEY] key:" + key, ErrorCode.ERRORCODE_INVALID_UPDATE_KEY_NOT_SUPPORTED_QDB.getMsg());
            LOGGER.error(ErrorCode.ERRORCODE_INVALID_UPDATE_KEY_NOT_SUPPORTED_QDB.getMsg() + ", key:" + key);
            return false;
        }
        return true;
    }

    @Override
    public boolean isValidQuery(Query query, QueryResponse queryResponse) {
        if (query == null) {
            queryResponse.status = Status.FAIL.getCode();
            queryResponse.error.put("[NULL_QUERY]", ErrorCode.ERRORCODE_NULL_QUERY.getMsg());
            LOGGER.error(ErrorCode.ERRORCODE_NULL_QUERY.getMsg());
            return false;
        }

        String key = query.getKey();
        Type type = query.getType();

        /**
         * Check if type is specified.
         */
        if (type == null) {
            queryResponse.status = Status.FAIL.getCode();
            queryResponse.error.put("[INVALID_QUERY_TYPE]", ErrorCode.ERRORCODE_INVALID_QUERY_TYPE.getMsg());
            LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY_TYPE.getMsg());
            return false;
        }

        /**
         * Check if there are missing parameters in query
         */
        if (StringUtils.isBlank(key)) {
            queryResponse.status = Status.FAIL.getCode();
            queryResponse.error.put("[MISSING_PARAMTER]=key", ErrorCode.ERRORCODE_INVALID_QUERY_MISSING_PARAMETER.getMsg() + ", parameter key is " +
                    "missing");
            LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY_MISSING_PARAMETER.getMsg() + ": key");

            /*if(metrics == null || metrics.isEmpty()) {
                queryResponse.error.put("[MISSING_METRICS]", ErrorCode.ERRORCODE_INVALID_QUERY_MISSING_PARAMETER.getMsg() + ", parameter metrics is
                 missing");
                LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY_MISSING_PARAMETER.getMsg() + ": metrics");
            }*/
            /*if(timePeriods == null || timePeriods.isEmpty()){
                queryResponse.error.put("[MISSING_PARAMETER]=tp", ErrorCode.ERRORCODE_INVALID_QUERY_MISSING_PARAMETER.getMsg() + ", parameter tp is
                 missing");
                LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY_MISSING_PARAMETER.getMsg() + ": tp");
            }*/
            return false;
        }

        /**
         * check if key is in valid format.
         * */
        if (!Utils.isAllDigits(key)) {
            queryResponse.status = Status.FAIL.getCode();
            queryResponse.error.put("[INVALID_KEY] key:" + key, ErrorCode.ERRORCODE_INVALID_QUERY_KEY_NOT_SUPPORTED_QDB.getMsg());
            LOGGER.error(ErrorCode.ERRORCODE_INVALID_QUERY_KEY_NOT_SUPPORTED_QDB.getMsg() + ", key:" + key);
            return false;
        }
        return true;
    }


    private void pipeLine(List<FlatKVEntry> flatKVEntryList, boolean async, int retry, List<Integer> availablePools, UpdateResponse updateResponse)
            throws ServiceException {

        int threadId = (int) Thread.currentThread().getId();

        /**
         * Have tried all 3 pools. All failed. return
         */
        if (retry == 3 || availablePools.size() == 0) {
            updateResponse.status = Status.FAIL.getCode();

            updateResponse.error.put(ErrorCode.ERRORCODE_QDB_WRITE_FAIL.getMsg(), "retry:" + retry);

            long end = System.currentTimeMillis();

//            MonitorUtil.recordMsg(end, "update/" + this.getClass().getSimpleName(), updateResponse, end - beginMap.get(threadId), StringUtils.join
//                    (flaMonitorUtil.recordMsg(end, "update/" + this.getClass().getSimpleName(), updateResponse, end - beginMap.get(threadId), StringUtils.join
//                    (flatKVEntryList, ","), "");

            throw new ServiceException(ErrorCode.ERRORCODE_QDB_WRITE_FAIL);

        }

        List<FlatKVEntry> commands = commandsMap.get(threadId);
        if (commands == null) {
            commands = new ArrayList<>();
            commandsMap.put(threadId, commands);
        }
        commands.addAll(flatKVEntryList);

        try {
            /**
             * If there is no pipeline in this thread, we new a Jedis instance and pipelined it.
             */
            if (jedisMap.get(threadId) == null || pipelineMap.get(threadId) == null) {
                Random random = new Random();
                int rand = random.nextInt(availablePools.size());
                Jedis jedis = JedisFactory.getInstance().getJedisPool(availablePools.get(rand)).getResource();
                jedisMap.put(threadId, jedis);
                resourceDistMap.put(threadId, rand);

                Pipeline pipeline = jedis.pipelined();

                pipelineMap.put(threadId, pipeline);
                beginMap.put(threadId, System.currentTimeMillis());
            }

            /**
             * Send commands to the current pipeline
             */
            Pipeline pipeline = pipelineMap.get(threadId);

            String lastKey = null;
            if (flatKVEntryList != null) {
                for (FlatKVEntry flatKVEntry : flatKVEntryList) {

                    String key = flatKVEntry.getKey();
                    HEntry hEntry = flatKVEntry.getHEntry();

                    if (hEntry.isCounter()) {
                        /*if(rand == 0) {
                            LOGGER.error("hincrByFloat: key=" + key + ", HEntry=" +hEntry.getHKey() + ", " + hEntry.getHVal());

                        }*/

                        pipeline.hincrByFloat(key.getBytes(), hEntry.getHKey().getBytes(), hEntry.getHVal().doubleValue());
                        if (lastKey == null || !key.equals(lastKey)) {
                            lastKey = key;
                            pipeline.expire(key.getBytes(), ConfManager.serverConfig.getExpireSec());
                        }
                    } else {
                        /*if(rand == 0) {
                            LOGGER.error("hset: key=" + key + ", HEntry=" +hEntry.getHKey() + ", " + hEntry.getHVal());

                        }*/
                        pipeline.hset(flatKVEntry.getKey().getBytes(), hEntry.getHKey().getBytes(), String.valueOf(hEntry.getHVal()).getBytes());

                    }
                }
            }

            /**
             * If count reaches a certain number or jedis connected time reaches certain length of time, or user set async = false,
             * we sync the pipeline, return the jedis to pool, and remove the pipeline associated with the threadId
             */
            if (commands.size() > 1000 || System.currentTimeMillis() - beginMap.get(threadId) > 10000 || !async) {
                long begin = System.currentTimeMillis();
                pipeline.sync();
                long end = System.currentTimeMillis();

                Random random = new Random();
                int rand = random.nextInt(100);
                if (rand == 0) {
                    LOGGER.error("[QDB] write count:" + commandsMap.get(threadId).size());
                    LOGGER.error("[QDB] pipeline sync spend:" + (end - begin) + "ms");

                }
                /*if(jedis != null) {
                    jedis.close();
                }*/
                // pipelineMap.remove(threadId);
                commands.clear();
                beginMap.put(threadId, end);

            }
        } catch (Exception e) {

            if (jedisMap.get(threadId) != null) {
                jedisMap.get(threadId).close();
            }
            pipelineMap.remove(threadId);
            List<FlatKVEntry> commandsCopy = new ArrayList(commands);
            commands.clear();

            updateResponse.status = Status.FAIL.getCode();

            updateResponse.error.put(ErrorCode.ERRORCODE_QDB_WRITE_FAIL.getMsg(), "retry:" + retry);

            long end = System.currentTimeMillis();

//            MonitorUtil.recordMsg(end, "update/" + this.getClass().getSimpleName(), updateResponse, end - beginMap.get(threadId), StringUtils.join
//                    (flatKVEntryList, ","), "");

            availablePools.remove(resourceDistMap.get(threadId));

            pipeLine(commandsCopy, false, retry++, availablePools, updateResponse);
        }


    }


    public static class HKeysClass {

        public List<String> hKeys;
        public List<DateTime> dateTimes;
        public List<String> metrics;

        public HKeysClass() {
            hKeys = new ArrayList<>();
            dateTimes = new ArrayList<>();
            metrics = new ArrayList<>();
        }

        public String toString() {
            return "HKeysClass:{" +
                    "hkeys:[" + StringUtils.join(hKeys, ",") +
                    "], dateTimes:[" + StringUtils.join(dateTimes, ",") +
                    "], metrics:[" + StringUtils.join(metrics, ",") + "]}";
        }
    }

}
