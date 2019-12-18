package com.atguigu.practice.model;

/**
 * 存储QDB中哈希表中的字段+值
 * Created by linghongbo on 2016/3/22.
 */
public class HEntry {

    private String hKey;
    private Number hVal;

    private boolean isCounter;

    public HEntry() {
    }

    public HEntry(String hKey, Number hVal, boolean isCounter) {
        this.hKey = hKey;
        this.hVal = hVal;
        this.isCounter = isCounter;
    }

    public String getHKey() {
        return hKey;
    }

    public void setHKey(String hKey) {
        this.hKey = hKey;
    }

    public Number getHVal() {
        return hVal;
    }

    public void setHVal(Number hVal) {
        this.hVal = hVal;
    }

    public boolean isCounter() {
        return isCounter;
    }

    @Override
    public String toString() {
        return "HEntry:{hKey:" + hKey +
                ", hVal:" + hVal +
                ", isCounter:" + isCounter + "}";
    }
}
