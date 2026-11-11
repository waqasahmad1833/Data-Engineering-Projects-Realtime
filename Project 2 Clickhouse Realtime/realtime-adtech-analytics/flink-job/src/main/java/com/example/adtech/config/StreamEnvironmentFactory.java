package com.example.adtech.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Execution environment tuned for a high-volume, long-running job. The state backend
 * (RocksDB, incremental checkpoints to S3) is set in the cluster configuration, not in code,
 * so the same jar runs locally and on Kubernetes.
 */
public final class StreamEnvironmentFactory {

    private StreamEnvironmentFactory() {
    }

    public static StreamExecutionEnvironment create(JobConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Every type must have a Flink-native serializer; no silent Kryo fallback.
        env.getConfig().disableGenericTypes();

        env.enableCheckpointing(config.getCheckpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setMinPauseBetweenCheckpoints(config.getCheckpointIntervalMs() / 2);
        checkpoints.setCheckpointTimeout(Duration.ofMinutes(10).toMillis());
        checkpoints.setTolerableCheckpointFailureNumber(3);
        checkpoints.setMaxConcurrentCheckpoints(1);
        // Checkpoints stay ALIGNED: Flink does not allow unaligned checkpoints together with the
        // custom shard partitioner in front of the ClickHouse sinks (rescaling could misplace
        // in-flight records). Barrier alignment is kept fast under backpressure with network
        // buffer debloating instead (taskmanager.network.memory.buffer-debloat.enabled).
        checkpoints.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Time.seconds(1), Time.minutes(5), 2.0, Time.hours(1), 0.1));
        return env;
    }
}
