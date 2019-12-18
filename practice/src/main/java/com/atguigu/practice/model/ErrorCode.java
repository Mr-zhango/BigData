package com.atguigu.practice.model;

public enum ErrorCode {
    ERRORCODE_EXCEPTION(10001, "exception"),
    ERRORCODE_FAIL_TO_CAST_STRING_TO_LONG(10002, "string convert to long fail"),
    ERRORCODE_TIMEINTERVAL_OUT_OF_BOUND(10003, "time interval out of bound"),
    ERRORCODE_TRANSFORMER_CLASS_NOT_FOUND(10004, "transformer class not found"),
    ERRORCODE_INVALID_TIMEDIMENSION(10005, "invalid timenimension"),
    ERRORCODE_INVALID_QUERY_METRIC_NOT_SUPPORTED(10006, "invalid query metirc"),
    ERRORCODE_INVALID_QUERY_TIMEPERIOD_NOT_SUPPORTED(10007, "invalid query timeperiod"),
    ERRORCODE_INVALID_QUERY_MISSING_PARAMETER(10008, "invalid query mission param"),
    ERRORCODE_INVALID_EVENT(10009, "invalid event"),
    ERRORCODE_INVALID_QUERY(10010, "invalid query"),
    ERRORCODE_QDB_PIPE_SYNC_FAIL(10011, "qdb pipe sync fail"),
    ERRORCODE_NULL_EVENT(10012, "null event"),
    ERRORCODE_INVALID_UPDATE_MISSING_PARAMETER(10013, "update miss param"),
    ERRORCODE_INVALID_UPDATE_KEY_NOT_SUPPORTED_QDB(10014, "key not support"),
    ERRORCODE_NULL_QUERY(10015, "null query"),
    ERRORCODE_INVALID_QUERY_TYPE(10016, "invalid query type"),
    ERRORCODE_INVALID_QUERY_KEY_NOT_SUPPORTED_QDB(10017, "invalid query key"),
    ERRORCODE_QDB_WRITE_FAIL(10018, "qdb write fail"),
    ERRORCODE_QDB_READ_FAIL(10019, "qdb read fail"),
    ERRORCODE_INVALID_EVENT_TYPE(10020, "invalid event type"),
    ERRORCODE_LOAD_TOPIC_CONFIG_FAIL(10021, "load topic config fail"),
    ERRORCODE_NULL_TOPIC_CONFIG(10022, "null topic config"),
    ERRORCODE_INVALID_TOPIC_CONFIG(10023, "invalid topic config"),
    ERRORCODE_NULL_PROCESSOR(10024, "null processor"),
    ERRORCODE_SHUTDOWN_INTERRUPTED(10025, "shutdown interrupted"),
    ERRORCODE_WRITE_FAIL(10026, "write fail"),
    ERRORCODE_FAIL_TO_PARSE_ARGUMENTS(10027, "fail to parse arguments"),
    ERRORCODE_LOAD_CONSUMER_PROPS_FAIL(10028, "load consumer props fail"),
    ERRORCODE_FAIL_TO_CAST_STRING_TO_INT(100029, "fail to cast string to int");
    private int code;
    private String msg;

    ErrorCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }
}
