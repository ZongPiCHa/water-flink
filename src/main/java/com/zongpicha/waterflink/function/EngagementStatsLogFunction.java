package com.zongpicha.waterflink.function;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zongpicha.waterflink.model.EngagementStats;

public final class EngagementStatsLogFunction
        implements MapFunction<EngagementStats, EngagementStats> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(EngagementStatsLogFunction.class);
    private static final DateTimeFormatter WINDOW_TIME_FORMAT = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("Asia/Shanghai"));

    @Override
    public EngagementStats map(EngagementStats stats) {
        LOG.info(
                "Engagement stats: scope={}, window_type={}, user_id={}, project_id={}, "
                        + "window_start={}, window_end={}, likes={}, comments={}, "
                        + "shares={}, follows={}",
                stats.getScope(),
                stats.getWindowType(),
                stats.getUserId(),
                stats.getProjectId(),
                WINDOW_TIME_FORMAT.format(Instant.ofEpochMilli(stats.getWindowStart())),
                WINDOW_TIME_FORMAT.format(Instant.ofEpochMilli(stats.getWindowEnd())),
                stats.getLikes(),
                stats.getComments(),
                stats.getShares(),
                stats.getFollows());
        return stats;
    }
}
