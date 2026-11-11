package com.example.adtech.sink;

import java.io.Serializable;

/** Per-table batching settings. A batch is flushed when any limit is reached, and on every checkpoint. */
public final class ShardedSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String table;
    private final int batchRows;
    private final long batchBytes;
    private final long flushIntervalMs;

    public ShardedSinkConfig(String table, int batchRows, long batchBytes, long flushIntervalMs) {
        if (table == null || !table.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("Invalid table name: " + table);
        }
        if (batchRows <= 0 || batchBytes <= 0 || flushIntervalMs <= 0) {
            throw new IllegalArgumentException("Batch limits must be > 0");
        }
        this.table = table;
        this.batchRows = batchRows;
        this.batchBytes = batchBytes;
        this.flushIntervalMs = flushIntervalMs;
    }

    public String getTable() { return table; }
    public int getBatchRows() { return batchRows; }
    public long getBatchBytes() { return batchBytes; }
    public long getFlushIntervalMs() { return flushIntervalMs; }
}
