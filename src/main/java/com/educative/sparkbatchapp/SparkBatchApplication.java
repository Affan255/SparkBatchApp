package com.educative.sparkbatchapp;

import com.educative.sparkbatchapp.exception.ExceptionHandler;
import com.educative.sparkbatchapp.job.Job;
import com.educative.sparkbatchapp.job.implementation.SparkJob;
import com.educative.sparkbatchapp.parameter.CommonJobParameter;
import com.educative.sparkbatchapp.parameter.JobsParameters;
import com.educative.sparkbatchapp.parameter.Parameters;
import com.educative.sparkbatchapp.persistence.entity.JobConfiguration;
import com.educative.sparkbatchapp.persistence.repository.ConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;

import java.util.Optional;

@SpringBootApplication
public class SparkBatchApplication {

    private static Logger LOGGER = LoggerFactory.getLogger(SparkBatchApplication.class);

    public static void main(String[] args) {
        LOGGER.info("Starting batch application...");
        SpringApplication.run(SparkBatchApplication.class, args);
        LOGGER.info("Batch application finished...");
    }


}
