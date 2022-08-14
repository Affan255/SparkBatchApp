package com.educative.sparkbatchapp.parameter;

public enum IngesterJobParams implements Parameter<IngesterJobParams>{
    READ_FORMAT("readFormat"),
    FILE_NAME("fileName");

    private String paramName;

    IngesterJobParams(String paramName) {
        this.paramName = paramName;
    }

    @Override
    public String getParamName() {
        return paramName;
    }
}
