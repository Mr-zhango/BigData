package com.atguigu.practice.model;

import java.util.Map;

/**
 */
public class UpdateResponse extends Response {

    public UpdateResponse() {
        super();
    }

    public UpdateResponse(int status, Map<String, String> error) {
        super(status, error);
    }
}
