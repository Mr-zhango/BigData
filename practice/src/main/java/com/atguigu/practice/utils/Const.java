package com.atguigu.practice.utils;

import java.util.regex.Pattern;

/**
 * Created by didi on 17/10/19.
 */
public class Const {
    public static final String CNT_ = "cnt_";
    public static final String AMT_ = "amt_";
    public static final String DIST_ = "dist_";
    public static final String LAST_ = "last_";

    public static final String CALL = "call";
    public static final String STRIVE = "strive";
    public static final String FINISH = "finish";
    public static final String PAY = "pay";
    public static final String PAID = "paid";
    public static final String TAXI = "taxi";

    public static final String UNDERSCORE = "_";
    public static final String DIST_FINISH = DIST_ + FINISH;
    public static final String LAST_FINISH = LAST_ + FINISH;
    public static final String CNT_CALL = CNT_ + CALL;
    public static final String CNT_STRIVE = CNT_ + STRIVE;
    public static final String LAST_FINISH_PRODUCT = LAST_FINISH + "_product";
    public static final String LAST_FINISH_AREA = LAST_FINISH + "_area";
    public static final String CNT_FINISH = CNT_ + FINISH;

    public final static String TODAY = "TODAY";
    public final static String YESTERDAY = "YESTERDAY";
    public final static String LASTNDAYS = "LAST(\\d+)DAYS";
    public final static String LASTNDAY = "LAST(\\d+)(?:ST|ND|RD|TH)?DAY";
    public final static String LASTMDAYTOLASTNDAY =
            "LAST(\\d+)(?:ST|ND|RD|TH)?DAYTOLAST(\\d+)(?:ST|ND|RD|TH)?DAY";
    public final static String YYYY_MM_DDTOYYYY_MM_DD =
            "(\\d{4}\\-\\d{2}\\-\\d{2})~(\\d{4}\\-\\d{2}\\-\\d{2})";
    public final static String YY_MM_DD = "\\d{4}\\-\\d{2}\\-\\d{2}";
    public final static String YY_MM_DD_HH_MM_SS = "\\d{4}\\-\\d{2}\\-\\d{2} \\d{2}:\\d{2}:\\d{2}";
    public final static String YY_MM_DD_T_HH_MM_SS = "\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}";

    public final static String THISWEEK = "THISWEEK";
    public final static String LASTWEEK = "LASTWEEK";
    public final static String LASTNWEEKS = "LAST(\\d+)WEEKS";
    public final static String LASTNWEEK = "LAST(\\d+)(?:ST|ND|RD|TH)?WEEK";
    public final static String LASTMWEEKTOLASTNWEEK =
            "LAST(\\d+)(?:ST|ND|RD|TH)?WEEKTOLAST(\\d+)(?:ST|ND|RD|TH)?WEEK";

    public final static String THISMONTH = "THISMONTH";
    public final static String LASTMONTH = "LASTMONTH";
    public final static String LASTNMONTHS = "LAST(\\d+)MONTHS";
    public final static String LASTNMONTH = "LAST(\\d+)(?:ST|ND|RD|TH)?MONTH";
    public final static String LASTMMONTHTOLASTNMONTH =
            "LAST(\\d+)(?:ST|ND|RD|TH)?MONTHTOLAST(\\d+)(?:ST|ND|RD|TH)?MONTH";

    public final static Pattern LASTNDAYS_PATTERN = Pattern.compile(LASTNDAYS);
    public final static Pattern LASTNDAY_PATTERN = Pattern.compile(LASTNDAY);
    public final static Pattern LASTMDAYTOLASTNDAY_PATTERN = Pattern.compile(LASTMDAYTOLASTNDAY);
    public final static Pattern YYYY_MM_DDTOYYYY_MM_DD_PATTERN =
            Pattern.compile(YYYY_MM_DDTOYYYY_MM_DD);
    public final static Pattern YYYY_MM_DD_PATTERN = Pattern.compile(YY_MM_DD);
    public final static Pattern TIME_PATTERN = Pattern.compile(YY_MM_DD_HH_MM_SS);
    public final static Pattern TIME_T_PATTERN = Pattern.compile(YY_MM_DD_T_HH_MM_SS);

    public final static Pattern LASTNWEEKS_PATTERN = Pattern.compile(LASTNWEEKS);
    public final static Pattern LASTNWEEK_PATTERN = Pattern.compile(LASTNWEEK);
    public final static Pattern LASTMWEEKTOLASTNWEEK_PATTERN = Pattern.compile(LASTMWEEKTOLASTNWEEK);

    public final static Pattern LASTNMONTHS_PATTERN = Pattern.compile(LASTNMONTHS);
    public final static Pattern LASTNMONTH_PATTERN = Pattern.compile(LASTNMONTH);
    public final static Pattern LASTMMONTHTOLASTNMONTH_PATTERN =
            Pattern.compile(LASTMMONTHTOLASTNMONTH);


}
