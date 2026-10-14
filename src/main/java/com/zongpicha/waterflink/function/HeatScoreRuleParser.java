package com.zongpicha.waterflink.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zongpicha.waterflink.model.HeatScoreRule;

public final class HeatScoreRuleParser
        implements FlatMapFunction<String, HeatScoreRule> {

    private static final long serialVersionUID = 1L;
    private static final long MAX_WEIGHT = 1_000_000L;
    private static final Logger LOG = LoggerFactory.getLogger(HeatScoreRuleParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<HeatScoreRule> output) {
        try {
            output.collect(parse(value));
        } catch (JsonProcessingException | IllegalArgumentException exception) {
            LOG.warn("Ignored malformed heat score rule: {}", exception.getMessage());
        }
    }

    static HeatScoreRule parse(String value) throws JsonProcessingException {
        JsonNode root = OBJECT_MAPPER.readTree(value);
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("rule must be a JSON object");
        }

        return new HeatScoreRule(
                requiredPositiveLong(root, "version"),
                requiredWeight(root, "like_weight"),
                requiredWeight(root, "comment_weight"),
                requiredWeight(root, "share_weight"),
                requiredWeight(root, "follow_weight"));
    }

    private static long requiredPositiveLong(JsonNode parent, String fieldName) {
        JsonNode value = parent.get(fieldName);
        if (value == null || !value.canConvertToLong() || value.longValue() <= 0L) {
            throw new IllegalArgumentException(fieldName + " must be a positive integer");
        }
        return value.longValue();
    }

    private static long requiredWeight(JsonNode parent, String fieldName) {
        JsonNode value = parent.get(fieldName);
        if (value == null
                || !value.canConvertToLong()
                || value.longValue() < 0L
                || value.longValue() > MAX_WEIGHT) {
            throw new IllegalArgumentException(
                    fieldName + " must be an integer between 0 and " + MAX_WEIGHT);
        }
        return value.longValue();
    }
}
