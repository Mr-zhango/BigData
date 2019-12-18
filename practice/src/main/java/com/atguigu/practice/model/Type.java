package com.atguigu.practice.model;

/**
 * Created by yiqunliu on 7/27/16.
 */
public enum Type {
    ALL(-1, "", "all"),
    PASSENGER(0, "P", "passenger"),
    GS_DRIVER(1, "D", "gs_driver"),
    TAXI_DRIVER(2, "T", "taxi_driver"),
    GRID(3, "A", "grid"),
    /**
     * server to city-area
     */
    CITY(4, "C", "city");

    private int code;
    private String sCode;
    private String lCode;

    Type(int code, String sCode, String lCode) {
        this.code = code;
        this.sCode = sCode;
        this.lCode = lCode;
    }

    public int getCode() {
        return code;
    }

    public String getsCode() {
        return sCode;
    }

    public String getlCode() {
        return lCode;
    }

    public static Type getType(String name) {

        Type type = null;
        if ("passenger".equals(name)) {
            type = PASSENGER;
        } else if ("gs_driver".equals(name)) {
            type = GS_DRIVER;
        } else if("taxi_driver".equals(name)) {
            type = TAXI_DRIVER;
        }else if ("grid".equals(name)) {
            type = GRID;
        }
        else if ("city".equals(name)){
            type = CITY;
        }
        return type;
    }


}
