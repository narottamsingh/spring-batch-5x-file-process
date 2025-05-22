package com.bce.batch.readerwritterprocessor;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;

import com.bce.batch.dto.Customer;

public class CustomerProcessor implements ItemProcessor<Customer,Customer> {
    private StepExecution stepExecution;	
    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
    	System.out.println("before steps:..............");
        this.stepExecution = stepExecution;
    }
    
    
    @Override
    public Customer process(Customer customer) throws Exception {
    	System.out.println(stepExecution.getExecutionContext().toString());
        	//System.out.println("Processor---- "+customer.getFirstName().length()+" -- "+Thread.currentThread().getName());
        	return customer;
    }
}
