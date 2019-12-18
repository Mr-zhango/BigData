package com.atguigu.practice.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by linghongbo.
 */
public class Response {

    public int status;
    public Map<String, String> error;

    public Response() {
        status = Status.SUCCESS.getCode();
        error = new HashMap<>();
    }

    public Response(int status, Map<String, String> error) {
        this.status = status;
        this.error = error;
    }
}
