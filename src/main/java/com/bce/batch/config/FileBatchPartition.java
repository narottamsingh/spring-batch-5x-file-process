package com.bce.batch.config;

import java.io.IOException;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.mapping.*;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.core.io.*;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.bce.batch.dto.Customer;
import com.bce.batch.readerwritterprocessor.*;

@Configuration
@EnableBatchProcessing
public class FileBatchPartition {

    @Autowired
    private  ResourcePatternResolver resourcePatternResolver;
    @Autowired
    private  JobRepository jobRepository;
    @Autowired
    private  PlatformTransactionManager transactionManager;

    private final String filePathDir = "/home/narottam/project/springboot/springbatch/spring-batch-file-process/src/main/resources/";

    @Bean
    public Step slaveStep1() {
        return new StepBuilder("slaveStep1", jobRepository)
                .<Customer, Customer>chunk(10, transactionManager)
                .reader(itemReader1(null))
                .processor(new CustomerProcessor())
                .writer(writer1())
                .taskExecutor(taskExecutorThread1())
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    public Step masterStep1() {
        return new StepBuilder("masterStep1", jobRepository)
                .partitioner("slaveStep2", partitioner1())
                .step(slaveStep1())
                .taskExecutor(taskExecutorThread1())
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> itemReader1(@Value("#{stepExecutionContext['filename']}") String filePath) {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(filePathDir + filePath));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper1());
        return itemReader;
    }

    @Bean
    public LineMapper<Customer> lineMapper1() {
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        return lineMapper;
    }

    @Bean
    public ItemWriter<Customer> writer1() {
        ResourceAwareItemWriterItemStreamImpl delegate = new ResourceAwareItemWriterItemStreamImpl();
        CustomerWriter<Customer> writer = new CustomerWriter<>();
        writer.setDelegate(delegate);
        writer.setResource(new FileSystemResource("output/customers_output.txt"));
        return writer;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutorThread1() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setQueueCapacity(10);
        executor.setThreadNamePrefix("partition-thread-");
        executor.initialize();
        return executor;
    }

    @Bean
    public CustomMultiResourcePartitioner partitioner1() {
        CustomMultiResourcePartitioner partitioner = new CustomMultiResourcePartitioner();
        Resource[] resources;
        try {
            resources = resourcePatternResolver.getResources("file:src/main/resources/*.csv");
        } catch (IOException e) {
            throw new RuntimeException("I/O problems when resolving the input file pattern.", e);
        }
        partitioner.setResources(resources);
        return partitioner;
    }

    @Bean
    public Job jobwithFilePartictionCsv() {
        return new JobBuilder("jobwithFilePartictionCsv", jobRepository)
                .start(masterStep1())
                .build();
    }
}
