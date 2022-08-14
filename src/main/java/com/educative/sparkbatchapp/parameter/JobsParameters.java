package com.educative.sparkbatchapp.parameter;

import java.util.Arrays;
import java.util.Optional;

public enum JobsParameters {
    INGERSTER_JOB("ingesterJob", IngesterJobParams.values()),
    SALES_SUMMARY_JOB("salesSummaryJob", SalesSummaryJobParams.values());

    private String jobName;
    private Parameter[] parameters;

    JobsParameters(String jobName, Parameter[] parameters) {
        this.jobName = jobName;
        this.parameters = parameters;
    }

    public Parameter[] getParameters() {
        return parameters;
    }

    public static Optional<JobsParameters> getParametersForJob(String job) {
        return Arrays.stream(values())
                .filter(jobsParameters ->  jobsParameters.jobName.equals(job))
                .findFirst();
    }
}
