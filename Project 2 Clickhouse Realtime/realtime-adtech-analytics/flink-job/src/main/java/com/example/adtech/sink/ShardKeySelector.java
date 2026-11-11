package com.example.adtech.sink;

import java.io.Serializable;

/** Extracts the sharding key; must equal the sharding expression of the Distributed table. */
@FunctionalInterface
public interface ShardKeySelector<T> extends Serializable {

    long shardKey(T value);
}
