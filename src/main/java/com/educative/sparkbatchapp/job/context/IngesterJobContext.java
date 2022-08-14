package com.educative.sparkbatchapp.job.context;

import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class IngesterJobContext extends CommonJobContext implements Serializable {
    public static final String JSON_FORMAT = "json";
    public static final String CSV_FORMAT = "csv";
}
