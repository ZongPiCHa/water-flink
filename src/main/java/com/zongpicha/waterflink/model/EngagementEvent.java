package com.zongpicha.waterflink.model;

import java.io.Serializable;

public class EngagementEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private long timestamp;
    private String logId;
    private String action;
    private String actorUserId;
    private String targetUserId;
    private String projectId;
    private String commentId;

    public EngagementEvent() {
    }

    public EngagementEvent(
            long timestamp,
            String logId,
            String action,
            String actorUserId,
            String targetUserId,
            String projectId,
            String commentId) {
        this.timestamp = timestamp;
        this.logId = logId;
        this.action = action;
        this.actorUserId = actorUserId;
        this.targetUserId = targetUserId;
        this.projectId = projectId;
        this.commentId = commentId;
    }

    public String relationKey() {
        return switch (action) {
            case "like", "unlike" -> "like|" + actorUserId + "|" + projectId;
            case "comment", "delete_comment" -> "comment|" + commentId;
            case "follow" -> "follow|" + actorUserId + "|" + targetUserId;
            case "share" -> "share|" + logId;
            default -> throw new IllegalStateException("Unsupported action: " + action);
        };
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLogId() {
        return logId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getActorUserId() {
        return actorUserId;
    }

    public void setActorUserId(String actorUserId) {
        this.actorUserId = actorUserId;
    }

    public String getTargetUserId() {
        return targetUserId;
    }

    public void setTargetUserId(String targetUserId) {
        this.targetUserId = targetUserId;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getCommentId() {
        return commentId;
    }

    public void setCommentId(String commentId) {
        this.commentId = commentId;
    }
}
