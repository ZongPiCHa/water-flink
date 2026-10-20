package com.zongpicha.waterflink;

import java.time.Duration;
import java.util.Locale;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zongpicha.waterflink.dimension.DimensionEnrichment;
import com.zongpicha.waterflink.function.DirtyProcessingTimeTrigger;
import com.zongpicha.waterflink.function.EffectiveEngagementFunction;
import com.zongpicha.waterflink.function.EngagementAggregateFunction;
import com.zongpicha.waterflink.function.EngagementEventParser;
import com.zongpicha.waterflink.function.EngagementWindowFunction;
import com.zongpicha.waterflink.function.HeatScoreBroadcastFunction;
import com.zongpicha.waterflink.function.HeatScoreRuleParser;
import com.zongpicha.waterflink.model.EngagementEvent;
import com.zongpicha.waterflink.model.EngagementStats;
import com.zongpicha.waterflink.model.HeatScoreRule;
import com.zongpicha.waterflink.model.MetricDelta;
import com.zongpicha.waterflink.model.StatsScope;
import com.zongpicha.waterflink.model.WindowType;
import com.zongpicha.waterflink.sink.EngagementJdbcSinks;

public final class WaterFlinkApplication {

    private static final Logger LOG = LoggerFactory.getLogger(WaterFlinkApplication.class);
    private static final Duration ALLOWED_LATENESS = Duration.ofMinutes(5);
    private static final Duration STATS_EMIT_INTERVAL = Duration.ofMinutes(1);
    private static final Duration SHANGHAI_DAY_OFFSET = Duration.ofHours(-8);
    private static final Duration SHANGHAI_WEEK_OFFSET = Duration.ofHours(-80);

    private WaterFlinkApplication() {
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "10.37.215.3:9092");
        String topic = env("KAFKA_TOPIC", "community.events.v1");
        String groupId = env("KAFKA_GROUP_ID", "water-flink-debug-v1");
        String startingOffsets = env("KAFKA_STARTING_OFFSETS", "latest")
                .toLowerCase(Locale.ROOT);
        String heatConfigTopic =
                env("HEAT_CONFIG_TOPIC", "water.heat-score-config.v1");
        String heatConfigGroupId =
                env("HEAT_CONFIG_GROUP_ID", "water-flink-heat-config-v1");
        String mysqlJdbcUrl = requiredEnv("MYSQL_JDBC_URL");
        String mysqlUser = requiredEnv("MYSQL_USER");
        String mysqlPassword = requiredEnv("MYSQL_PASSWORD");
        String hudiDimensionPath = env(
                "HUDI_DIMENSION_PATH",
                "hdfs://10.37.215.3:8020/hudi/water");
        String dimensionCacheTtl = env("DIMENSION_CACHE_TTL", "10 min");
        int statsParallelism = Integer.parseInt(
                env("FLINK_STATS_PARALLELISM", "6"));
        if (statsParallelism < 1) {
            throw new IllegalArgumentException(
                    "FLINK_STATS_PARALLELISM must be greater than zero");
        }

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
        environment.setParallelism(statsParallelism);
        environment.enableCheckpointing(120_000L, CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setCheckpointTimeout(600_000L);
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        environment.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        environment.getCheckpointConfig().enableUnalignedCheckpoints();
        environment.getCheckpointConfig()
                .setAlignedCheckpointTimeout(Duration.ofSeconds(30));
        environment.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();

        KafkaSource<String> heatConfigSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(heatConfigTopic)
                .setGroupId(heatConfigGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        LOG.info(
                "Starting water-flink: bootstrapServers={}, topic={}, groupId={}, "
                        + "offsets={}, heatConfigTopic={}, statsParallelism={}",
                bootstrapServers,
                topic,
                groupId,
                startingOffsets,
                heatConfigTopic,
                statsParallelism);

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

        MapStateDescriptor<String, HeatScoreRule> heatRuleState =
                new MapStateDescriptor<>(
                        "active-heat-score-rule",
                        String.class,
                        HeatScoreRule.class);
        BroadcastStream<HeatScoreRule> heatRules = environment
                .fromSource(
                        heatConfigSource,
                        WatermarkStrategy.noWatermarks(),
                        "Heat score config source")
                .name("Heat score config source")
                .uid("heat-score-config-source")
                .setParallelism(1)
                .flatMap(new HeatScoreRuleParser())
                .name("Parse heat score config")
                .uid("parse-heat-score-config")
                .broadcast(heatRuleState);

        DataStream<EngagementStats> engagementStats = aggregateStats(
                metricDeltas,
                StatsScope.USER,
                WindowType.HOUR,
                TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .union(
                        aggregateStats(
                                metricDeltas,
                                StatsScope.USER,
                                WindowType.DAY,
                                TumblingEventTimeWindows.of(
                                        Duration.ofDays(1),
                                        SHANGHAI_DAY_OFFSET)),
                        aggregateStats(
                                metricDeltas,
                                StatsScope.USER,
                                WindowType.THREE_DAY,
                                TumblingEventTimeWindows.of(
                                        Duration.ofDays(3),
                                        SHANGHAI_DAY_OFFSET)),
                        aggregateStats(
                                metricDeltas,
                                StatsScope.USER,
                                WindowType.WEEK,
                                TumblingEventTimeWindows.of(
                                        Duration.ofDays(7),
                                        SHANGHAI_WEEK_OFFSET)),
                        aggregateStats(
                                metricDeltas,
                                StatsScope.PROJECT,
                                WindowType.HOUR,
                                TumblingEventTimeWindows.of(Duration.ofHours(1))),
                        aggregateStats(
                                metricDeltas,
                                StatsScope.PROJECT,
                                WindowType.DAY,
                                TumblingEventTimeWindows.of(
                                        Duration.ofDays(1),
                                        SHANGHAI_DAY_OFFSET)),
                        aggregateStats(
                                metricDeltas,
                                StatsScope.PROJECT,
                                WindowType.THREE_DAY,
                                TumblingEventTimeWindows.of(
                                        Duration.ofDays(3),
                                        SHANGHAI_DAY_OFFSET)),
                        aggregateStats(
                                metricDeltas,
                                StatsScope.PROJECT,
                                WindowType.WEEK,
                                TumblingEventTimeWindows.of(
                                        Duration.ofDays(7),
                                        SHANGHAI_WEEK_OFFSET)));

        DataStream<EngagementStats> scoredStats = engagementStats
                .connect(heatRules)
                .process(new HeatScoreBroadcastFunction(heatRuleState))
                .name("Calculate heat score")
                .uid("calculate-heat-score");

        DataStream<EngagementStats> enrichedStats = DimensionEnrichment.enrich(
                StreamTableEnvironment.create(environment),
                scoredStats,
                hudiDimensionPath,
                dimensionCacheTtl);

        enrichedStats
                .filter(stats -> stats.getScope() == StatsScope.USER)
                .name("Select user stats")
                .uid("select-user-stats")
                .sinkTo(EngagementJdbcSinks.userSink(
                        mysqlJdbcUrl,
                        mysqlUser,
                        mysqlPassword))
                .name("Upsert user stats")
                .uid("upsert-user-stats");

        enrichedStats
                .filter(stats -> stats.getScope() == StatsScope.PROJECT)
                .name("Select project stats")
                .uid("select-project-stats")
                .keyBy(EngagementStats::getProjectId)
                .sinkTo(EngagementJdbcSinks.projectSink(
                        mysqlJdbcUrl,
                        mysqlUser,
                        mysqlPassword))
                .name("Upsert project stats")
                .uid("upsert-project-stats")
                .setParallelism(statsParallelism);

        environment.execute("water-flink-engagement-stats");
    }

    private static DataStream<EngagementStats> aggregateStats(
            DataStream<MetricDelta> metricDeltas,
            StatsScope scope,
            WindowType windowType,
            TumblingEventTimeWindows windowAssigner) {
        KeyedStream<MetricDelta, String> keyedDeltas = scope == StatsScope.USER
                ? metricDeltas.keyBy(MetricDelta::getTargetUserId)
                : metricDeltas.keyBy(MetricDelta::getProjectId);
        String scopeName = scope.name().toLowerCase(Locale.ROOT);
        String windowName = windowType.name().toLowerCase(Locale.ROOT);

        return keyedDeltas
                .window(windowAssigner)
                .trigger(DirtyProcessingTimeTrigger.of(STATS_EMIT_INTERVAL))
                .allowedLateness(ALLOWED_LATENESS)
                .aggregate(
                        new EngagementAggregateFunction(),
                        new EngagementWindowFunction(scope, windowType))
                .name("Aggregate " + scopeName + " " + windowName + " stats")
                .uid("aggregate-" + scopeName + "-" + windowName + "-stats");
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }
}
