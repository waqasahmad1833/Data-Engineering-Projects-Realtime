package com.example.adtech.sink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ClickHouse cluster topology and connection settings.
 *
 * <p>Shards are given as a spec string: shards separated by {@code ;}, replicas of a shard by
 * {@code ,}. Shard order must match the {@code remote_servers} order in ClickHouse, because the
 * sink computes {@code shard = key % shards} exactly like the Distributed table engine.
 *
 * <pre>http://ch-s1-r1:8123,http://ch-s1-r2:8123;http://ch-s2-r1:8123,http://ch-s2-r2:8123</pre>
 */
public final class ClickHouseClusterConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ArrayList<ArrayList<String>> shards;
    private final String user;
    private final String password;
    private final String database;
    private final boolean gzip;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final long connectTimeoutMs;
    private final long requestTimeoutMs;

    private ClickHouseClusterConfig(Builder b) {
        this.shards = parseShards(b.shards);
        this.user = b.user;
        this.password = b.password == null ? "" : b.password;
        this.database = b.database;
        this.gzip = b.gzip;
        this.maxRetries = b.maxRetries;
        this.retryBackoffMs = b.retryBackoffMs;
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.requestTimeoutMs = b.requestTimeoutMs;
        if (user == null || user.isBlank()) {
            throw new IllegalArgumentException("ClickHouse user must be set");
        }
        if (database == null || !database.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("Invalid ClickHouse database: " + database);
        }
        if (maxRetries < 0 || retryBackoffMs < 0) {
            throw new IllegalArgumentException("Invalid retry settings");
        }
    }

    static ArrayList<ArrayList<String>> parseShards(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("ClickHouse shard spec is empty");
        }
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        // limit -1 keeps trailing empty entries, so a typo like "a;b;" fails loudly instead of
        // silently changing the shard count (which would route rows to the wrong shards)
        for (String shard : spec.split(";", -1)) {
            ArrayList<String> replicas = new ArrayList<>();
            for (String replica : shard.split(",")) {
                String url = replica.trim();
                if (url.isEmpty()) {
                    continue;
                }
                if (!url.startsWith("http://") && !url.startsWith("https://")) {
                    throw new IllegalArgumentException("Replica URL must start with http(s)://: " + url);
                }
                replicas.add(url.endsWith("/") ? url.substring(0, url.length() - 1) : url);
            }
            if (replicas.isEmpty()) {
                throw new IllegalArgumentException("Shard without replicas in spec: " + spec);
            }
            result.add(replicas);
        }
        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int numShards() {
        return shards.size();
    }

    public List<String> replicas(int shard) {
        return Collections.unmodifiableList(shards.get(shard));
    }

    public String getUser() { return user; }
    public String getPassword() { return password; }
    public String getDatabase() { return database; }
    public boolean isGzip() { return gzip; }
    public int getMaxRetries() { return maxRetries; }
    public long getRetryBackoffMs() { return retryBackoffMs; }
    public long getConnectTimeoutMs() { return connectTimeoutMs; }
    public long getRequestTimeoutMs() { return requestTimeoutMs; }

    @Override
    public String toString() {
        return "ClickHouseCluster{shards=" + shards + ", user='" + user + "', database='" + database
                + "', gzip=" + gzip + ", maxRetries=" + maxRetries + '}';
    }

    public static final class Builder {
        private String shards;
        private String user = "default";
        private String password = "";
        private String database = "default";
        private boolean gzip = true;
        private int maxRetries = 6;
        private long retryBackoffMs = 500L;
        private long connectTimeoutMs = 10_000L;
        private long requestTimeoutMs = 120_000L;

        private Builder() {
        }

        public Builder shards(String shards) { this.shards = shards; return this; }
        public Builder user(String user) { this.user = user; return this; }
        public Builder password(String password) { this.password = password; return this; }
        public Builder database(String database) { this.database = database; return this; }
        public Builder gzip(boolean gzip) { this.gzip = gzip; return this; }
        public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
        public Builder retryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; return this; }
        public Builder connectTimeoutMs(long ms) { this.connectTimeoutMs = ms; return this; }
        public Builder requestTimeoutMs(long ms) { this.requestTimeoutMs = ms; return this; }

        public ClickHouseClusterConfig build() {
            return new ClickHouseClusterConfig(this);
        }
    }
}
