package com.zongpicha.waterflink.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.zongpicha.waterflink.model.EngagementEvent;

class EngagementEventParserTest {

    @Test
    void parsesProjectEngagementAndUsesAuthorAsTarget() throws Exception {
        EngagementEvent event = EngagementEventParser.parse("""
                {
                  "timestamp": 1791912945307,
                  "log_id": "log-1",
                  "action": "comment",
                  "user_id": "actor-1",
                  "properties": {
                    "project_id": "project-1",
                    "author_user_id": "author-1",
                    "comment_id": "comment-1"
                  }
                }
                """);

        assertEquals("comment", event.getAction());
        assertEquals("actor-1", event.getActorUserId());
        assertEquals("author-1", event.getTargetUserId());
        assertEquals("project-1", event.getProjectId());
        assertEquals("comment-1", event.getCommentId());
        assertEquals("comment|comment-1", event.relationKey());
    }

    @Test
    void parsesFollowAndKeepsProjectAttribution() throws Exception {
        EngagementEvent event = EngagementEventParser.parse("""
                {
                  "timestamp": 1791912945337,
                  "log_id": "log-2",
                  "action": "follow",
                  "user_id": "actor-2",
                  "properties": {
                    "project_id": "project-2",
                    "target_user_id": "target-2"
                  }
                }
                """);

        assertEquals("target-2", event.getTargetUserId());
        assertEquals("project-2", event.getProjectId());
        assertEquals("follow|actor-2|target-2", event.relationKey());
    }

    @Test
    void ignoresUnrelatedActionsAndRejectsMissingDimensions() throws Exception {
        assertNull(EngagementEventParser.parse("""
                {
                  "timestamp": 1791912945337,
                  "log_id": "log-3",
                  "action": "view",
                  "user_id": "actor-3",
                  "properties": {}
                }
                """));

        assertThrows(IllegalArgumentException.class, () -> EngagementEventParser.parse("""
                {
                  "timestamp": 1791912945337,
                  "log_id": "log-4",
                  "action": "like",
                  "user_id": "actor-4",
                  "properties": {
                    "project_id": "project-4"
                  }
                }
                """));
    }
}
