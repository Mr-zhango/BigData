package com.atguigu.practice.model;

public enum TimePeriod {

    TODAY("TO", "TODAY"),
    YESTERDAY("YE", "YESTERDAY"),
    THISWEEK("TW", "THISWEEK"),
    LASTWEEK("LW", "LASTWEEK"),
    THISMONTH("TM", "THISMONTH"),
    LASTMONTH("LM", "LASTMONTH"),
    LAST7DAYS("L7D", "LAST7DAYS"),
    LAST30DAYS("L30D", "LAST30DAYS");

    private String sCode;
    private String lCode;

    TimePeriod(String s, String l) {

        sCode = s;
        lCode = l;
    }

    public String getLCode() {
        return lCode;
    }

    public String getSCode() {
        return sCode;
    }

}
