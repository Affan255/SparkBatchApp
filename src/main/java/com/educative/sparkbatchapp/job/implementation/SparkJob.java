package com.educative.sparkbatchapp.job.implementation;

import com.educative.sparkbatchapp.job.Job;
import com.educative.sparkbatchapp.parameter.CommonJobParameter;
import com.educative.sparkbatchapp.parameter.Parameters;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SparkJob extends Job<Dataset<Row>> {

    private static Logger LOGGER = LoggerFactory.getLogger(SparkJob.class);

    private Parameters parameters;

    @Autowired
    SparkJob(Parameters parameters) {
//        LOGGER.info(parameters.getParamValue(CommonJobParameter.JOB_NAME));
        this.parameters = parameters;
    }

    @Override
    protected Dataset<Row> preProcess() {
//        LOGGER.info(parameters.getParamValue(CommonJobParameter.JOB_NAME));
        return null;
    }

    @Override
    protected Dataset<Row> process(Dataset<Row> preProcessOutput) {
        return null;
    }

    @Override
    protected void postProcess(Dataset<Row> processOutput) {

    }
}
