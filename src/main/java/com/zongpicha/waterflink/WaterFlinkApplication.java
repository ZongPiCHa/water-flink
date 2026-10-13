package com.zongpicha.waterflink;

import java.time.Duration;
import java.util.Locale;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zongpicha.waterflink.function.EffectiveEngagementFunction;
import com.zongpicha.waterflink.function.EngagementAggregateFunction;
import com.zongpicha.waterflink.function.EngagementEventParser;
import com.zongpicha.waterflink.function.UserMinuteWindowFunction;
import com.zongpicha.waterflink.function.UserStatsLogFunction;
import com.zongpicha.waterflink.model.EngagementEvent;
import com.zongpicha.waterflink.model.MetricDelta;
import com.zongpicha.waterflink.model.UserEngagementStats;

public final class WaterFlinkApplication {

    private static final Logger LOG = LoggerFactory.getLogger(WaterFlinkApplication.class);

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

        LOG.info(
                "Starting water-flink: bootstrapServers={}, topic={}, groupId={}, offsets={}",
                bootstrapServers,
                topic,
                groupId,
                startingOffsets);

        DataStream<EngagementEvent> engagementEvents = environment
                .fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka source")
                .name("Kafka source")
                .uid("kafka-source")
                .flatMap(new EngagementEventParser())
                .name("Parse engagement events")
                .uid("parse-engagement-events")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EngagementEvent>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(20))
                                .withTimestampAssigner(
                                        (event, previousTimestamp) ->
                                                event.getTimestamp())
                                .withIdleness(Duration.ofMinutes(1)))
                .name("Assign event time")
                .uid("assign-event-time");

        DataStream<MetricDelta> metricDeltas = engagementEvents
                .keyBy(EngagementEvent::relationKey)
                .process(new EffectiveEngagementFunction())
                .name("Resolve effective engagement")
                .uid("resolve-effective-engagement");

        DataStream<UserEngagementStats> minuteStats = metricDeltas
                .keyBy(MetricDelta::getTargetUserId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        new EngagementAggregateFunction(),
                        new UserMinuteWindowFunction())
                .name("Aggregate user minute stats")
                .uid("aggregate-user-minute-stats");

        minuteStats
                .map(new UserStatsLogFunction())
                .name("Log user minute stats")
                .uid("log-user-minute-stats")
                .sinkTo(new DiscardingSink<>())
                .name("Discard sink")
                .uid("discard-sink");

        environment.execute("water-flink-user-engagement-stats");
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }
}
