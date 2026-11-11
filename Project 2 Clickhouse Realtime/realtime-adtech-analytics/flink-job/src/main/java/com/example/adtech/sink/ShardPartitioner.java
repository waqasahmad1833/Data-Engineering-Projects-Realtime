package com.example.adtech.sink;

import org.apache.flink.api.common.functions.Partitioner;

/** Routes writer index {@code i} to sink subtask {@code i % parallelism}. */
public class ShardPartitioner implements Partitioner<Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public int partition(Integer writer, int numPartitions) {
        return writer % numPartitions;
    }
}
