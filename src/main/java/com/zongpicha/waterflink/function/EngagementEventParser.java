package com.zongpicha.waterflink.function;

import java.util.Locale;
import java.util.Set;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zongpicha.waterflink.model.EngagementEvent;

public final class EngagementEventParser
        extends RichFlatMapFunction<String, EngagementEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(EngagementEventParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Set<String> SUPPORTED_ACTIONS = Set.of(
            "like",
            "unlike",
            "comment",
            "delete_comment",
            "share",
            "follow",
            "view_page");

    private transient Counter ignoredEvents;
    private transient Counter malformedEvents;

    @Override
    public void open(OpenContext openContext) {
        ignoredEvents = getRuntimeContext()
                .getMetricGroup()
                .counter("ignored_events");
        malformedEvents = getRuntimeContext()
                .getMetricGroup()
                .counter("malformed_events");
    }

    @Override
    public void flatMap(String value, Collector<EngagementEvent> output) {
        try {
            EngagementEvent event = parse(value);
            if (event == null) {
                ignoredEvents.inc();
                return;
            }
            output.collect(event);
        } catch (JsonProcessingException | IllegalArgumentException exception) {
            malformedEvents.inc();
            LOG.warn("Ignored malformed engagement event: {}", exception.getMessage());
        }
    }

    static EngagementEvent parse(String value) throws JsonProcessingException {
        JsonNode root = OBJECT_MAPPER.readTree(value);
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("event must be a JSON object");
        }

        String action = requiredText(root, "action").toLowerCase(Locale.ROOT);
        if (!SUPPORTED_ACTIONS.contains(action)) {
            // Other project-scoped actions use additive PV semantics. They do not
            // affect heat until explicitly included in the versioned weight map.
            JsonNode properties = root.path("properties");
            if (!action.matches("[a-z][a-z0-9_]{0,47}")
                    || properties.path("project_id").asText("").isBlank()
                    || properties.path("author_user_id").asText("").isBlank()) {
                return null;
            }
        }

        long timestamp = requiredLong(root, "timestamp");
        String logId = requiredText(root, "log_id");
        String actorUserId = requiredText(root, "user_id");
        JsonNode properties = root.get("properties");
        if (properties == null || !properties.isObject()) {
            throw new IllegalArgumentException("missing object field: properties");
        }

        String projectId = requiredText(properties, "project_id");
        String targetUserId;
        String commentId = null;

        if ("follow".equals(action)) {
            targetUserId = requiredText(properties, "target_user_id");
        } else {
            targetUserId = requiredText(properties, "author_user_id");
            if ("comment".equals(action) || "delete_comment".equals(action)) {
                commentId = requiredText(properties, "comment_id");
            }
        }

        return new EngagementEvent(
                timestamp,
                logId,
                action,
                actorUserId,
                targetUserId,
                projectId,
                commentId);
    }

    private static String requiredText(JsonNode parent, String fieldName) {
        JsonNode value = parent.get(fieldName);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException("missing text field: " + fieldName);
        }
        return value.textValue();
    }

    private static long requiredLong(JsonNode parent, String fieldName) {
        JsonNode value = parent.get(fieldName);
        if (value == null || !value.canConvertToLong()) {
            throw new IllegalArgumentException("missing integer field: " + fieldName);
        }
        long result = value.longValue();
        if (result <= 0L) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
        return result;
    }
}
