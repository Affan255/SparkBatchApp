package com.educative.sparkbatchapp.parameter;

import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Component
public class Parameters {
    private static final String ARGS_SEPARATOR = "=";
    private static final int KEY = 0;
    private static final int VALUE = 1;

    private Map<Parameter,String> jobParams = new HashMap<>();

    public void loadAllParams(String[] appArgs, Parameter[] parameters) {

        Map<String,String> keyValues = new HashMap<>();
        Arrays.stream(appArgs).forEach(arg -> {
            String[] keyValue = arg.split(ARGS_SEPARATOR);
            keyValues.put(keyValue[KEY], keyValue[VALUE]);
        });
        Arrays.stream(parameters).forEach(parameter -> {
            if (keyValues.containsKey(parameter.getParamName()))
                jobParams.put(parameter,keyValues.get(parameter.getParamName()));
        });
    }

    public String getParamValue(Parameter parameter) {
        return jobParams.get(parameter);
    }
}
