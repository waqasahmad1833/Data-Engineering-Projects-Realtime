package com.example.adtech.sink;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardRoutingTest {

    @Test
    void matchesClickHouseModuloForUnsignedKeys() {
        assertEquals(0, ShardRouter.shardFor(10, 2));
        assertEquals(1, ShardRouter.shardFor(11, 2));
        assertEquals(3, ShardRouter.shardFor(4611686018427387907L, 4));
        // -1L is 18446744073709551615 as UInt64 in ClickHouse; 18446744073709551615 % 4 = 3
        assertEquals(3, ShardRouter.shardFor(-1L, 4));
    }

    @Test
    void assignerSendsEachShardToItsOwnWriters() {
        ShardWriterAssigner<Long> assigner = new ShardWriterAssigner<>(v -> v, 2, 3);
        ShardPartitioner partitioner = new ShardPartitioner();
        for (long key = 0; key < 1000; key++) {
            int writer = assigner.getKey(key);
            int subtask = partitioner.partition(writer, 6);
            assertEquals(ShardRouter.shardFor(key, 2), subtask / 3, "subtask must serve the key's shard");
        }
    }

    @Test
    void assignerSpreadsLoadAcrossWritersOfAShard() {
        ShardWriterAssigner<Long> assigner = new ShardWriterAssigner<>(v -> 0L, 2, 4);
        int[] counts = new int[4];
        for (int i = 0; i < 4000; i++) {
            counts[assigner.getKey(0L)]++;
        }
        for (int c : counts) {
            assertEquals(1000, c);
        }
    }

    @Test
    void parsesShardSpec() {
        ClickHouseClusterConfig cfg = ClickHouseClusterConfig.builder()
                .shards(" http://a1:8123, http://a2:8123/ ; http://b1:8123 ")
                .user("u").database("adtech").build();
        assertEquals(2, cfg.numShards());
        assertEquals("http://a2:8123", cfg.replicas(0).get(1));
        assertEquals(1, cfg.replicas(1).size());
    }

    @Test
    void rejectsBadShardSpecs() {
        assertThrows(IllegalArgumentException.class,
                () -> ClickHouseClusterConfig.builder().shards("a1:8123").user("u").database("d").build());
        assertThrows(IllegalArgumentException.class,
                () -> ClickHouseClusterConfig.builder().shards("http://a:1;;").user("u").database("d").build());
        assertThrows(IllegalArgumentException.class,
                () -> ClickHouseClusterConfig.builder().shards("http://a:1").user("u").database("x;drop").build());
    }

    @Test
    void shardBufferTracksRowsBytesAndAge() {
        ShardBuffer buffer = new ShardBuffer();
        assertTrue(buffer.isEmpty());
        buffer.add("{\"a\":1}", 1000);
        buffer.add("{\"a\":2}", 1500);
        assertEquals(2, buffer.rowCount());
        assertEquals("{\"a\":1}\n{\"a\":2}", buffer.payload());
        assertEquals(15, buffer.sizeBytes());
        assertEquals(1000, buffer.ageMillis(2000));
        buffer.clear();
        assertTrue(buffer.isEmpty());
        assertEquals(0, buffer.ageMillis(5000));
    }
}
