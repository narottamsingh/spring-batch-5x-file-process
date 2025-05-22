package com.bce.batch.config;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

public class SetupTasklet implements Tasklet {
    
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Your task logic goes here
        System.out.println("Executing my setup tasklet...");
        // Return RepeatStatus.FINISHED if the task is successful
        Thread.sleep(10000);
        System.out.println("Executing my setup tasklet done........");
        return RepeatStatus.FINISHED;
    }
}