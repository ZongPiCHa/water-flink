package com.zongpicha.waterflink;

import java.util.Locale;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class WaterFlinkApplication {

    private WaterFlinkApplication() {
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "10.37.215.3:9092");
        String topic = env("KAFKA_TOPIC", "community.events.v1");
        String groupId = env("KAFKA_GROUP_ID", "water-flink-debug-v1");
        String startingOffsets = env("KAFKA_STARTING_OFFSETS", "latest")
                .toLowerCase(Locale.ROOT);

        OffsetsInitializer offsetsInitializer = switch (startingOffsets) {
            case "earliest" ->
                    OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
            case "latest" ->
                    OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
            default -> throw new IllegalArgumentException(
                    "KAFKA_STARTING_OFFSETS must be earliest or latest");
        };

        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setCheckpointTimeout(60_000L);
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();

        System.out.printf(
                "Starting water-flink: bootstrapServers=%s, topic=%s, groupId=%s, offsets=%s%n",
                bootstrapServers,
                topic,
                groupId,
                startingOffsets);

        environment
                .fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka source")
                .name("Kafka source")
                .uid("kafka-source")
                .print("water-flink")
                .name("stdout sink")
                .uid("stdout-sink");

        environment.execute("water-flink-kafka-debug");
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }
}
