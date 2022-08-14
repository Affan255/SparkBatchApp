package com.educative.sparkbatchapp.exception;

import com.educative.sparkbatchapp.exception.model.BatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ExceptionHandler {
    private static Logger LOGGER = LoggerFactory.getLogger(ExceptionHandler.class);

    public void handleException(Throwable exception) {
        LOGGER.info("Non Business exception occurred: " + exception.getMessage());
    }

    public void handleBusinessException(BatchException exception) {
        LOGGER.info("Business exception occurred: " + exception.getMessage());
    }

}
