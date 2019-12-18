package com.atguigu.practice.kafka;

public enum KafkaTopic {
    G_ORDER("g_order"),

    TAXI_ORDER("test");


    private String code;

    KafkaTopic(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static KafkaTopic getType(String topic) {

        KafkaTopic kafkaTopic = null;

        switch(topic) {
            case "g_order":
                kafkaTopic = G_ORDER;
                break;
            case "test":
                kafkaTopic = TAXI_ORDER;
                break;
        }

        return kafkaTopic;
    }
}
