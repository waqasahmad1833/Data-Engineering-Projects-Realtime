package com.example.clickstream.sink;

import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Immutable settings for one {@link ClickHouseSink}. */
public final class ClickHouseSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String database;
    private final String table;
    private final String user;
    private final String password;
    private final int batchSize;
    private final long flushIntervalMs;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final long connectTimeoutMs;
    private final long requestTimeoutMs;

    private ClickHouseSinkConfig(Builder b) {
        this.url = Objects.requireNonNull(b.url, "url");
        this.database = Objects.requireNonNull(b.database, "database");
        this.table = Objects.requireNonNull(b.table, "table");
        this.user = Objects.requireNonNull(b.user, "user");
        this.password = b.password == null ? "" : b.password;
        this.batchSize = b.batchSize;
        this.flushIntervalMs = b.flushIntervalMs;
        this.maxRetries = b.maxRetries;
        this.retryBackoffMs = b.retryBackoffMs;
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.requestTimeoutMs = b.requestTimeoutMs;
        if (!database.matches("[A-Za-z0-9_]+") || !table.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("database and table must be plain identifiers");
        }
        if (batchSize <= 0 || flushIntervalMs <= 0 || maxRetries < 0 || retryBackoffMs < 0) {
            throw new IllegalArgumentException("invalid batching/retry settings");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public String qualifiedTable() {
        return database + "." + table;
    }

    /** HTTP endpoint for {@code INSERT INTO db.table FORMAT JSONEachRow}. */
    public URI insertUri() {
        String query = "INSERT INTO " + qualifiedTable() + " FORMAT JSONEachRow";
        String base = url.endsWith("/") ? url : url + "/";
        String encoded = URLEncoder.encode(query, StandardCharsets.UTF_8).replace("+", "%20");
        return URI.create(base + "?query=" + encoded + "&date_time_input_format=best_effort");
    }

    public String getUrl() { return url; }
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public String getUser() { return user; }
    public String getPassword() { return password; }
    public int getBatchSize() { return batchSize; }
    public long getFlushIntervalMs() { return flushIntervalMs; }
    public int getMaxRetries() { return maxRetries; }
    public long getRetryBackoffMs() { return retryBackoffMs; }
    public long getConnectTimeoutMs() { return connectTimeoutMs; }
    public long getRequestTimeoutMs() { return requestTimeoutMs; }

    public static final class Builder {
        private String url;
        private String database;
        private String table;
        private String user = "default";
        private String password = "";
        private int batchSize = 5000;
        private long flushIntervalMs = 2000L;
        private int maxRetries = 5;
        private long retryBackoffMs = 500L;
        private long connectTimeoutMs = 10_000L;
        private long requestTimeoutMs = 60_000L;

        private Builder() {
        }

        public Builder url(String url) { this.url = url; return this; }
        public Builder database(String database) { this.database = database; return this; }
        public Builder table(String table) { this.table = table; return this; }
        public Builder user(String user) { this.user = user; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder batchSize(int batchSize) { this.batchSize = batchSize; return this; }
        public Builder flushIntervalMs(long flushIntervalMs) { this.flushIntervalMs = flushIntervalMs; return this; }
        public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
        public Builder retryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; return this; }
        public Builder connectTimeoutMs(long connectTimeoutMs) { this.connectTimeoutMs = connectTimeoutMs; return this; }
        public Builder requestTimeoutMs(long requestTimeoutMs) { this.requestTimeoutMs = requestTimeoutMs; return this; }

        public ClickHouseSinkConfig build() {
            return new ClickHouseSinkConfig(this);
        }
    }
}
