package com.atguigu.practice.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yiqunliu on 6/23/16.
 */
public class QueryResponse extends Response {

    public Map<String, Object> value;

    public QueryResponse(Map<String, String> error, int status, Map<String, Object> value) {
        super(status, error);
        this.value = value;
    }

    public QueryResponse() {
        super();
        value = new HashMap<>();
    }

}
