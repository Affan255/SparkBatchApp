package com.educative.sparkbatchapp.job.context;

import com.educative.sparkbatchapp.parameter.Parameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public abstract class CommonJobContext {

    @Autowired
    private Parameters parameters;

    @Value("${spark.master.mode}")
    private String sparkMode;

    public Parameters getParameters() {
        return parameters;
    }

    public String getSparkMode() {
        return sparkMode;
    }
}
