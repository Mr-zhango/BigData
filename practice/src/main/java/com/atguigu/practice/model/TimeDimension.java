package com.atguigu.practice.model;

public enum TimeDimension {

    MINUTELY(10, "m", "MINUTELY"),
    FIVE_MINUTELY(20, "5m", "FIVE_MINUTELY"),
    HOURLY(30, "H", "HOURLY"),
    DAILY(40, "D", "DAILY"),
    MONTHLY(50, "M", "MONTHLY");

    private int code;
    private String sCode;
    private String lCode;

    TimeDimension(int c, String s, String l) {
        code = c;
        sCode = s;
        lCode = l;
    }

    public String getLCode() {
        return lCode;
    }

    public int getCode() {
        return code;
    }

    public String getSCode() {
        return sCode;
    }

    public static TimeDimension getFromSCode(String code) {
        if(code == null) {
            return null;
        }
        TimeDimension timeDimension = null;
        switch(code) {
            case "m":
                timeDimension = MINUTELY;
                break;
            case "5m":
                timeDimension = FIVE_MINUTELY;
                break;
            case "H":
                timeDimension = HOURLY;
                break;
            case "D":
                timeDimension = DAILY;
                break;
            case "M":
                timeDimension = MONTHLY;
                break;

        }
        return timeDimension;
    }
}
