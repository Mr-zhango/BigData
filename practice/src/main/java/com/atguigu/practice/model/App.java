package com.atguigu.practice.model;

public enum App {
    DIDI("didi"),
    UBER("uber");

    private String code;
    App(String code) {
        this.code = code;
    }

    String getCode() {
        return code;
    }

    public static App getApp(String name) {
        if(name != null) {
            name = name.toLowerCase();
        }
        App app = DIDI;
        if ("uber".equals(name)) {
            app = UBER;
        }
        return app;
    }

}
