package com.example.adtech.sink;

/**
 * Shard selection identical to ClickHouse's Distributed engine with equal shard weights:
 * {@code shard_index = sharding_key % number_of_shards} on the unsigned value.
 */
public final class ShardRouter {

    private ShardRouter() {
    }

    public static int shardFor(long shardKey, int numShards) {
        if (numShards <= 0) {
            throw new IllegalArgumentException("numShards must be > 0");
        }
        return (int) Long.remainderUnsigned(shardKey, numShards);
    }
}
