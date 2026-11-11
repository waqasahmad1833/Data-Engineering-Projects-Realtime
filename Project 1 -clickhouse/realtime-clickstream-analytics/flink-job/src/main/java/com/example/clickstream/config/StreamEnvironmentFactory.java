package com.example.clickstream.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Creates a {@link StreamExecutionEnvironment} with checkpointing and restart behaviour configured. */
public final class StreamEnvironmentFactory {

    private static final int RESTART_ATTEMPTS = 10;
    private static final long CHECKPOINT_TIMEOUT_MS = 120_000L;

    private StreamEnvironmentFactory() {
    }

    public static StreamExecutionEnvironment create(JobConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Fail fast if any type would silently fall back to (slow, fragile) Kryo serialization.
        env.getConfig().disableGenericTypes();

        // Checkpoints drive Kafka offset commits and force the ClickHouse sinks to flush.
        env.enableCheckpointing(config.getCheckpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setMinPauseBetweenCheckpoints(config.getCheckpointIntervalMs() / 2);
        checkpoints.setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
        checkpoints.setTolerableCheckpointFailureNumber(3);
        checkpoints.setMaxConcurrentCheckpoints(1);
        checkpoints.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(RESTART_ATTEMPTS, Time.seconds(10)));
        return env;
    }
}
