package com.zongpicha.waterflink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.zongpicha.waterflink.model.EngagementStats;
import com.zongpicha.waterflink.model.HeatScoreRule;

class HeatScoreRuleTest {

    @Test
    void dynamicWeightsSupportViewsAndFutureMetrics() throws Exception {
        HeatScoreRule rule = HeatScoreRuleParser.parse("""
                {"schema_version":2,"version":10,
                 "weights":{"like":1000,"view_page":100,"download":50}}
                """);
        EngagementStats stats = new EngagementStats();
        stats.setLikes(2);
        stats.setExtraMetrics(Map.of("view_page", 10L, "download", 4L));
        assertEquals(3200, rule.calculate(stats));
        stats.setExtraMetrics(null);
        assertEquals(2000, rule.calculate(stats));
    }

    @Test
    void rejectsFractionalUnknownSchemaAndInvalidMetricRules() {
        for (String json : new String[] {
                "{\"schema_version\":2,\"version\":1,\"weights\":{\"like\":0.1}}",
                "{\"schema_version\":3,\"version\":1,\"weights\":{\"like\":100}}",
                "{\"schema_version\":2,\"version\":1,\"weights\":{\"unlike\":100}}",
                "{\"schema_version\":2,\"version\":1,\"weights\":{}}",
                "{\"schema_version\":2,\"version\":1.5,\"weights\":{\"view_page\":100}}"
        }) {
            assertThrows(IllegalArgumentException.class, () -> HeatScoreRuleParser.parse(json));
        }
    }

    @Test
    void parsesRuleAndCalculatesFullSnapshotScore() throws Exception {
        HeatScoreRule rule = HeatScoreRuleParser.parse("""
                {
                  "version": 7,
                  "like_weight": 1000,
                  "comment_weight": 3000,
                  "share_weight": 5000,
                  "follow_weight": 4000
                }
                """);
        EngagementStats stats = new EngagementStats();
        stats.setLikes(2L);
        stats.setComments(3L);
        stats.setShares(1L);
        stats.setFollows(-1L);

        assertEquals(7L, rule.getVersion());
        assertEquals(12_000L, rule.calculate(stats));
    }

    @Test
    void rejectsInvalidRuleWeights() {
        assertThrows(IllegalArgumentException.class, () -> HeatScoreRuleParser.parse("""
                {
                  "version": 8,
                  "like_weight": -1,
                  "comment_weight": 3000,
                  "share_weight": 5000,
                  "follow_weight": 4000
                }
                """));
    }
}
