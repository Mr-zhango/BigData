package com.atguigu.practice.model;

/**
 * Created by yiqunliu on 6/30/16.
 */
public enum Status {

    SUCCESS(10000),
    FAIL(10001),
    PARTIALLY_FAIL(10002);

    private int code;

    Status(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }


}
