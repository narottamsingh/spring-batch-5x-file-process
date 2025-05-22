package com.bce.batch.config;

import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;

import java.util.HashMap;
import java.util.Map;

public class CustomMultiResourcePartitioner implements Partitioner {

	private Resource[] resources;

	public void setResources(Resource[] resources) {
		this.resources = resources;
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> partitions = new HashMap<>();
		int i = 0;
		for (Resource resource : resources) {
			ExecutionContext context = new ExecutionContext();
			context.putString("filename", resource.getFilename()); // this must match @Value key
			partitions.put("partition" + i, context);
			i++;
		}
		return partitions;
	}
}
