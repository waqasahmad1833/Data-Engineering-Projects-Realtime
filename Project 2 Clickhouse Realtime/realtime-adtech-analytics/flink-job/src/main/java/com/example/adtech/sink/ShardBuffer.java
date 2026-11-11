package com.example.adtech.sink;

/** Accumulates newline-delimited JSON rows for one shard. */
final class ShardBuffer {

    private final StringBuilder rows = new StringBuilder(1 << 20);
    private int rowCount;
    private long firstRowAtMillis;

    void add(String jsonRow, long nowMillis) {
        if (rowCount == 0) {
            firstRowAtMillis = nowMillis;
        } else {
            rows.append('\n');
        }
        rows.append(jsonRow);
        rowCount++;
    }

    boolean isEmpty() { return rowCount == 0; }
    int rowCount() { return rowCount; }
    /** Approximate payload size (chars ~ bytes for the ASCII-heavy JSON we produce). */
    long sizeBytes() { return rows.length(); }
    long ageMillis(long nowMillis) { return rowCount == 0 ? 0 : nowMillis - firstRowAtMillis; }
    String payload() { return rows.toString(); }

    void clear() {
        rows.setLength(0);
        rowCount = 0;
        firstRowAtMillis = 0;
    }
}
