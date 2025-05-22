package com.bce.batch.readerwritterprocessor;

import java.util.List;

import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;

public class CustomerWriter<T> implements ItemWriter<T> {

	private FlatFileItemWriter<T> delegate;
	private WritableResource resource;

	public void setDelegate(FlatFileItemWriter<T> delegate) {
		this.delegate = delegate;
	}

	public void setResource(WritableResource resource) {
		this.resource = resource;
		this.delegate.setResource(resource);
	}



	@Override
	public void write(Chunk<? extends T> chunk) throws Exception {
		delegate.write(chunk);
	}
}
