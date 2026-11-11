package com.example.adtech.sink;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * Assigns each record to a sink writer that serves only its target shard:
 * {@code writer = shard * writersPerShard + (round-robin within the shard)}.
 *
 * <p>Combined with {@link ShardPartitioner} and a sink parallelism of
 * {@code shards * writersPerShard}, every sink subtask writes to exactly one shard. Each writer
 * therefore accumulates large single-shard batches instead of many small per-shard batches,
 * which keeps the insert rate per shard low (ClickHouse prefers ~1 big insert per second).
 */
public class ShardWriterAssigner<T> implements KeySelector<T, Integer> {

    private static final long serialVersionUID = 1L;

    private final ShardKeySelector<T> shardKey;
    private final int numShards;
    private final int writersPerShard;
    private transient int roundRobin;

    public ShardWriterAssigner(ShardKeySelector<T> shardKey, int numShards, int writersPerShard) {
        if (numShards <= 0 || writersPerShard <= 0) {
            throw new IllegalArgumentException("numShards and writersPerShard must be > 0");
        }
        this.shardKey = shardKey;
        this.numShards = numShards;
        this.writersPerShard = writersPerShard;
    }

    @Override
    public Integer getKey(T value) {
        int shard = ShardRouter.shardFor(shardKey.shardKey(value), numShards);
        if (writersPerShard == 1) {
            return shard;
        }
        roundRobin = (roundRobin + 1) & Integer.MAX_VALUE;
        return shard * writersPerShard + roundRobin % writersPerShard;
    }
}
