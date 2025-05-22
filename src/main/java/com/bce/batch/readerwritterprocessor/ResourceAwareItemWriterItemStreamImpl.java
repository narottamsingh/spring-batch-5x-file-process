package com.bce.batch.readerwritterprocessor;

import org.springframework.batch.item.file.FlatFileItemWriter;

public class ResourceAwareItemWriterItemStreamImpl<T> extends FlatFileItemWriter<T> {
	public ResourceAwareItemWriterItemStreamImpl() {
		setAppendAllowed(true);
		setLineAggregator(Object::toString);  // Adjust based on your format
	}
}
