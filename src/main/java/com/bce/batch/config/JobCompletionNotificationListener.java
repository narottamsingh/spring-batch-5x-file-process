package com.bce.batch.config;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener implements JobExecutionListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    @Override
    public void beforeJob(JobExecution jobExecution) {
        LOGGER.info("Job {} is starting...", jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus().isUnsuccessful()) {
        	jobExecution.setStatus(BatchStatus.COMPLETED);
        	jobExecution.setExitStatus(ExitStatus.COMPLETED);
            LOGGER.error("Job {} failed with status: {}", jobExecution.getJobInstance().getJobName(), jobExecution.getStatus());
            System.exit(0);
        } else {
            LOGGER.info("Job {} completed successfully!", jobExecution.getJobInstance().getJobName());
        }
    }
}
