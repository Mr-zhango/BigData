package com.atguigu.practice.model;

public class ServiceException extends Exception{

    private static final long serialVersionUID = 1L;
    private ErrorCode errorCode;
    private String operation = "";

    public ServiceException(ErrorCode errorcode) {
        super();
        this.errorCode = errorcode;
    }

    public ServiceException(String operation, ErrorCode errorcode) {
        super();
        this.errorCode = errorcode;
        this.operation = operation;
    }


    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public String getDatasetName() {
        return operation;
    }

    public void setDatasetName(String operation) {
        this.operation = operation;
    }

    public String getErrorMsg() {
        return operation + " " + errorCode.getMsg();
    }
}