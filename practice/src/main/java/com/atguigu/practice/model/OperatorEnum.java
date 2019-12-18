package com.atguigu.practice.model;

/**
 * Created by linghongbo on 2016/3/23.
 */
public enum OperatorEnum {

    QDB(1,"qdb"),
    HBASE_GRID(2,"hbase_grid"),
    CASSANDRA(3,"cassandra"),
    HBASE_CITY_GRID(4, "hbase_city_grid");
    private int code;
    private String name;

    OperatorEnum(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public static OperatorEnum getType(String name) {
        OperatorEnum operatorEnum = null;
        if ("qdb".equals(name)) {
            operatorEnum = QDB;
        } else if ("hbase_grid".equals(name)) {
            operatorEnum = HBASE_GRID;
        }
        else if ("hbase_city_grid".equals(name)){
            operatorEnum = HBASE_CITY_GRID;
        }
        else if ("cassandra".equals(name)) {
            operatorEnum = CASSANDRA;
        }
        return operatorEnum;
    }


}
