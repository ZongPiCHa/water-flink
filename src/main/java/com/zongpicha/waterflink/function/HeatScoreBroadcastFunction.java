package com.zongpicha.waterflink.function;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zongpicha.waterflink.model.EngagementStats;
import com.zongpicha.waterflink.model.HeatScoreRule;

public final class HeatScoreBroadcastFunction
        extends BroadcastProcessFunction<
                EngagementStats,
                HeatScoreRule,
                EngagementStats> {

    private static final long serialVersionUID = 1L;
    private static final String ACTIVE_RULE_KEY = "active";
    private static final Logger LOG =
            LoggerFactory.getLogger(HeatScoreBroadcastFunction.class);

    private final MapStateDescriptor<String, HeatScoreRule> stateDescriptor;

    public HeatScoreBroadcastFunction(
            MapStateDescriptor<String, HeatScoreRule> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    @Override
    public void processElement(
            EngagementStats stats,
            ReadOnlyContext context,
            Collector<EngagementStats> output) throws Exception {
        ReadOnlyBroadcastState<String, HeatScoreRule> state =
                context.getBroadcastState(stateDescriptor);
        HeatScoreRule rule = state.get(ACTIVE_RULE_KEY);
        if (rule == null) {
            rule = HeatScoreRule.DEFAULT;
        }

        stats.setHeatScore(rule.calculate(stats));
        stats.setScoreVersion(rule.getVersion());
        output.collect(stats);
    }

    @Override
    public void processBroadcastElement(
            HeatScoreRule rule,
            Context context,
            Collector<EngagementStats> output) throws Exception {
        HeatScoreRule current =
                context.getBroadcastState(stateDescriptor).get(ACTIVE_RULE_KEY);
        if (current != null && rule.getVersion() < current.getVersion()) {
            return;
        }

        context.getBroadcastState(stateDescriptor).put(ACTIVE_RULE_KEY, rule);
        LOG.info(
                "Activated heat score rule: version={}, like={}, comment={}, share={}, follow={}",
                rule.getVersion(),
                rule.getLikeWeight(),
                rule.getCommentWeight(),
                rule.getShareWeight(),
                rule.getFollowWeight());
    }
}
