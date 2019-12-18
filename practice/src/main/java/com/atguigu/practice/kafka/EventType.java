package com.atguigu.practice.kafka;


public enum EventType {

    INSERT("i"),
    UPDATE("u"),
    DELETE("d");

    private String code;

    EventType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static EventType getEventType(String str) {

        if(str == null) {
            return null;
        }
        EventType eventType = null;

        switch(str) {
            case "i":
                eventType = INSERT;
                break;
            case "u":
                eventType = UPDATE;
                break;
        }
        return eventType;
    }
}