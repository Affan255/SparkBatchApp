package com.educative.sparkbatchapp.runner;

import com.educative.sparkbatchapp.SparkBatchApplication;
import com.educative.sparkbatchapp.exception.ExceptionHandler;
import com.educative.sparkbatchapp.job.Job;
import com.educative.sparkbatchapp.parameter.CommonJobParameter;
import com.educative.sparkbatchapp.parameter.JobsParameters;
import com.educative.sparkbatchapp.parameter.Parameters;
import com.educative.sparkbatchapp.persistence.entity.JobConfiguration;
import com.educative.sparkbatchapp.persistence.repository.ConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Optional;

@ConditionalOnProperty(
        prefix = "application.runner",
        value = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@Component
public class SparkBatchAppRunner implements CommandLineRunner {
    private static Logger LOGGER = LoggerFactory.getLogger(SparkBatchAppRunner.class);

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ExceptionHandler exceptionHandler;

    @Autowired
    ConfigRepository configRepository;

    @Autowired
    private Parameters parameters;

    @Override
    public void run(String... args) throws Exception {
        try {
            LOGGER.info("Loading job config...");

            JobConfiguration jobConfiguration = new JobConfiguration();
            jobConfiguration.setClientId("client1");
            jobConfiguration.setConfigName("inputPath");
            jobConfiguration.setJobName("ingesterJob");
            jobConfiguration.setVal("src/main/resources/data/ingestion/");
            configRepository.save(jobConfiguration);

            LOGGER.info("Parsing common job arguments...");
            loadCommonParams(args);
            LOGGER.info("Instantiating job...");
            Job job = (Job) context.getBean(parameters.getParamValue(CommonJobParameter.JOB_NAME), CommonJobParameter.JOB_NAME);
            LOGGER.info("Loading job specific arguments...");
            loadJobParams(args);
            LOGGER.info("Executing job...");
            LOGGER.info(String.valueOf(job));
            job.execute();
        } catch (Exception e) {
            exceptionHandler.handleException(e);
            throw e;
        }
    }

    private void loadCommonParams(String[] args) {
        parameters.loadAllParams(args, CommonJobParameter.values());
    }
    private void loadJobParams(String[] args) {
        String jobName = parameters.getParamValue(CommonJobParameter.JOB_NAME);
        Optional<JobsParameters> paramsFound = JobsParameters.getParametersForJob(jobName);
        if (paramsFound.isPresent())
            parameters.loadAllParams(args, paramsFound.get().getParameters());
    }
}
