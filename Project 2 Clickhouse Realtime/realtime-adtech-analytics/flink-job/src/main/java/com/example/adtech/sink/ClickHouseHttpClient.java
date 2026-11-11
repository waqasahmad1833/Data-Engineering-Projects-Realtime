package com.example.adtech.sink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

/**
 * Inserts batches into one shard over HTTP with replica failover.
 *
 * <p>Every batch gets an {@code insert_deduplication_token}. If a request times out after the
 * server already committed it and the retry goes to the <em>other</em> replica, the replicated
 * table recognises the token (stored in Keeper) and ignores the second copy, so transport
 * retries never create duplicates.
 */
public class ClickHouseHttpClient {

    private static final int MAX_ERROR_BODY_CHARS = 1000;
    private static final int MAX_BACKOFF_SHIFT = 6;

    private final ClickHouseClusterConfig cluster;
    private final HttpClient http;
    private final int replicaOffset;

    /**
     * @param replicaOffset spreads load: writer {@code i} starts with replica {@code i % replicas}
     */
    public ClickHouseHttpClient(ClickHouseClusterConfig cluster, int replicaOffset) {
        this.cluster = cluster;
        this.replicaOffset = Math.max(0, replicaOffset);
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(cluster.getConnectTimeoutMs()))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    /**
     * Inserts newline-delimited JSON rows into {@code database.table} on the given shard.
     *
     * @return number of attempts used (1 = first try succeeded)
     */
    public int insert(int shard, String table, String jsonRows, int rowCount) throws IOException, InterruptedException {
        List<String> replicas = cluster.replicas(shard);
        String token = UUID.randomUUID().toString();
        byte[] body = cluster.isGzip() ? gzip(jsonRows) : jsonRows.getBytes(StandardCharsets.UTF_8);

        IOException last = null;
        int attempts = cluster.getMaxRetries() + 1;
        for (int attempt = 0; attempt < attempts; attempt++) {
            String replica = replicas.get((replicaOffset + attempt) % replicas.size());
            try {
                send(replica, table, body, token);
                return attempt + 1;
            } catch (ClickHouseWriteException e) {
                if (!e.isRetryable()) {
                    throw e;
                }
                last = e;
            } catch (IOException e) {
                last = e;
            }
            // Back off only after every replica of the shard failed in this round.
            boolean roundComplete = (attempt + 1) % replicas.size() == 0;
            if (roundComplete && attempt + 1 < attempts) {
                int round = (attempt + 1) / replicas.size();
                Thread.sleep(cluster.getRetryBackoffMs() * (1L << Math.min(round - 1, MAX_BACKOFF_SHIFT)));
            }
        }
        throw new IOException(String.format("Insert of %d rows into %s.%s on shard %d failed after %d attempts",
                rowCount, cluster.getDatabase(), table, shard, attempts), last);
    }

    static URI insertUri(String replica, String database, String table, String token) {
        String query = "INSERT INTO " + database + "." + table + " FORMAT JSONEachRow";
        return URI.create(replica + "/?query=" + encode(query)
                + "&insert_deduplication_token=" + encode(token)
                + "&date_time_input_format=best_effort");
    }

    private void send(String replica, String table, byte[] body, String token) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(insertUri(replica, cluster.getDatabase(), table, token))
                .timeout(Duration.ofMillis(cluster.getRequestTimeoutMs()))
                .header("Content-Type", "application/x-ndjson; charset=utf-8")
                .header("X-ClickHouse-User", cluster.getUser())
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (!cluster.getPassword().isEmpty()) {
            request.header("X-ClickHouse-Key", cluster.getPassword());
        }
        if (cluster.isGzip()) {
            request.header("Content-Encoding", "gzip");
        }
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            String text = response.body() == null ? "" : response.body().trim();
            if (text.length() > MAX_ERROR_BODY_CHARS) {
                text = text.substring(0, MAX_ERROR_BODY_CHARS) + "...";
            }
            throw new ClickHouseWriteException(response.statusCode(), replica, text);
        }
    }

    static byte[] gzip(String text) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(Math.max(1024, text.length() / 6));
        try (GZIPOutputStream out = new GZIPOutputStream(bytes, 64 * 1024)) {
            out.write(text.getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
