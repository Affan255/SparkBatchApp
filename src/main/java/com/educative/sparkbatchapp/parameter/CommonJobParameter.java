package com.educative.sparkbatchapp.parameter;

import java.util.Arrays;
import java.util.Optional;

public enum CommonJobParameter implements Parameter<CommonJobParameter> {
    JOB_NAME("jobName"),
    CLIENT_ID("clientId");

    private String paramName;

    CommonJobParameter(String paramName) {
        this.paramName = paramName;
    }

    public Optional<CommonJobParameter> paramExists(String argName) {
        return Arrays.stream(CommonJobParameter.values())
                .filter(commonJobParameter -> commonJobParameter.getParamName().equals(argName))
                .findFirst();
    }

    @Override
    public String getParamName() {
        return paramName;
    }
}
