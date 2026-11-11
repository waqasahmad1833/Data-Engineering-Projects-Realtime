package com.example.adtech.sink;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseHttpClientTest {

    /** A fake ClickHouse replica that records requests and answers with scripted status codes. */
    static final class FakeReplica implements AutoCloseable {
        final HttpServer server;
        final List<String> tokens = Collections.synchronizedList(new ArrayList<>());
        final List<String> bodies = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger requests = new AtomicInteger();
        volatile int[] statuses;

        FakeReplica(int... statuses) throws IOException {
            this.statuses = statuses;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                int n = requests.getAndIncrement();
                String query = URLDecoder.decode(exchange.getRequestURI().getRawQuery(), StandardCharsets.UTF_8);
                for (String part : query.split("&")) {
                    if (part.startsWith("insert_deduplication_token=")) {
                        tokens.add(part.substring(part.indexOf('=') + 1));
                    }
                }
                byte[] raw = exchange.getRequestBody().readAllBytes();
                InputStream in = "gzip".equals(exchange.getRequestHeaders().getFirst("Content-Encoding"))
                        ? new GZIPInputStream(new ByteArrayInputStream(raw)) : new ByteArrayInputStream(raw);
                bodies.add(new String(in.readAllBytes(), StandardCharsets.UTF_8));
                int status = n < this.statuses.length ? this.statuses[n] : 200;
                byte[] reply = (status == 200 ? "" : "Code: 999. simulated error").getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, reply.length == 0 ? -1 : reply.length);
                if (reply.length > 0) {
                    exchange.getResponseBody().write(reply);
                }
                exchange.close();
            });
            server.start();
        }

        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static ClickHouseClusterConfig cluster(String spec, boolean gzip) {
        return ClickHouseClusterConfig.builder().shards(spec).user("adtech").password("pw")
                .database("adtech").gzip(gzip).maxRetries(3).retryBackoffMs(1).build();
    }

    @Test
    void sendsGzippedBatchWithToken() throws Exception {
        try (FakeReplica r1 = new FakeReplica()) {
            ClickHouseHttpClient client = new ClickHouseHttpClient(cluster(r1.url(), true), 0);
            int attempts = client.insert(0, "ad_events_local", "{\"a\":1}\n{\"a\":2}", 2);
            assertEquals(1, attempts);
            assertEquals("{\"a\":1}\n{\"a\":2}", r1.bodies.get(0));
            assertEquals(36, r1.tokens.get(0).length());
        }
    }

    @Test
    void failsOverToOtherReplicaWithTheSameToken() throws Exception {
        try (FakeReplica r1 = new FakeReplica(503); FakeReplica r2 = new FakeReplica()) {
            ClickHouseHttpClient client = new ClickHouseHttpClient(cluster(r1.url() + "," + r2.url(), false), 0);
            assertEquals(2, client.insert(0, "t", "{}", 1));
            assertEquals(1, r1.requests.get());
            assertEquals(1, r2.requests.get());
            assertEquals(r1.tokens.get(0), r2.tokens.get(0), "retry must reuse the dedup token");
        }
    }

    @Test
    void replicaOffsetSpreadsWritersAcrossReplicas() throws Exception {
        try (FakeReplica r1 = new FakeReplica(); FakeReplica r2 = new FakeReplica()) {
            ClickHouseClusterConfig cfg = cluster(r1.url() + "," + r2.url(), false);
            new ClickHouseHttpClient(cfg, 0).insert(0, "t", "{}", 1);
            new ClickHouseHttpClient(cfg, 1).insert(0, "t", "{}", 1);
            assertEquals(1, r1.requests.get());
            assertEquals(1, r2.requests.get());
        }
    }

    @Test
    void doesNotRetryClientErrors() throws Exception {
        try (FakeReplica r1 = new FakeReplica(400); FakeReplica r2 = new FakeReplica()) {
            ClickHouseHttpClient client = new ClickHouseHttpClient(cluster(r1.url() + "," + r2.url(), false), 0);
            ClickHouseWriteException e = assertThrows(ClickHouseWriteException.class,
                    () -> client.insert(0, "t", "{}", 1));
            assertEquals(400, e.getStatusCode());
            assertEquals(0, r2.requests.get());
        }
    }

    @Test
    void givesUpAfterMaxRetries() throws Exception {
        try (FakeReplica r1 = new FakeReplica(500, 500, 500, 500); FakeReplica r2 = new FakeReplica(500, 500, 500, 500)) {
            ClickHouseHttpClient client = new ClickHouseHttpClient(cluster(r1.url() + "," + r2.url(), false), 0);
            IOException e = assertThrows(IOException.class, () -> client.insert(0, "t", "{}", 1));
            assertTrue(e.getMessage().contains("after 4 attempts"));
            assertEquals(4, r1.requests.get() + r2.requests.get());
        }
    }

    @Test
    void buildsEncodedInsertUri() {
        String uri = ClickHouseHttpClient.insertUri("http://ch:8123", "adtech", "ad_events_local", "tok-1").toString();
        assertEquals("http://ch:8123/?query=INSERT%20INTO%20adtech.ad_events_local%20FORMAT%20JSONEachRow"
                + "&insert_deduplication_token=tok-1&date_time_input_format=best_effort", uri);
    }
}
