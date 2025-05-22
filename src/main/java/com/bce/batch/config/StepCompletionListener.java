package com.bce.batch.config;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class StepCompletionListener implements StepExecutionListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(StepCompletionListener.class);

    @Override
    public void beforeStep(StepExecution stepExecution) {
        LOGGER.info("Step {} is starting...", stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        LOGGER.info("Step {} completed with status: {}", stepExecution.getStepName(), stepExecution.getStatus());       
        return stepExecution.getExitStatus();
    }
    
}

