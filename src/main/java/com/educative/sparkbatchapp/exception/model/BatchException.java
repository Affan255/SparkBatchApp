package com.educative.sparkbatchapp.exception.model;

public class BatchException extends RuntimeException {
    public BatchException(String msg) {
        super(msg);
    }
    public BatchException(String msg, Throwable ex) {
        super(msg, ex);
    }
}
