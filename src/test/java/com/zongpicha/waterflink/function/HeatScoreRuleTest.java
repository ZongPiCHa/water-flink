package com.zongpicha.waterflink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.zongpicha.waterflink.model.EngagementStats;
import com.zongpicha.waterflink.model.HeatScoreRule;

class HeatScoreRuleTest {

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
