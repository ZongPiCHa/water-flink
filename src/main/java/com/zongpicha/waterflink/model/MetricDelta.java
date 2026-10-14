package com.zongpicha.waterflink.model;

import java.io.Serializable;

public class MetricDelta implements Serializable {

    private static final long serialVersionUID = 1L;

    private String targetUserId;
    private String projectId;
    private long likeDelta;
    private long commentDelta;
    private long shareDelta;
    private long followDelta;

    public MetricDelta() {
    }

    public MetricDelta(
            String targetUserId,
            String projectId,
            long likeDelta,
            long commentDelta,
            long shareDelta,
            long followDelta) {
        this.targetUserId = targetUserId;
        this.projectId = projectId;
        this.likeDelta = likeDelta;
        this.commentDelta = commentDelta;
        this.shareDelta = shareDelta;
        this.followDelta = followDelta;
    }

    public static MetricDelta like(EngagementEvent event, long delta) {
        return new MetricDelta(
                event.getTargetUserId(), event.getProjectId(), delta, 0L, 0L, 0L);
    }

    public static MetricDelta comment(EngagementEvent event, long delta) {
        return new MetricDelta(
                event.getTargetUserId(), event.getProjectId(), 0L, delta, 0L, 0L);
    }

    public static MetricDelta share(EngagementEvent event) {
        return new MetricDelta(
                event.getTargetUserId(), event.getProjectId(), 0L, 0L, 1L, 0L);
    }

    public static MetricDelta follow(EngagementEvent event) {
        return new MetricDelta(
                event.getTargetUserId(), event.getProjectId(), 0L, 0L, 0L, 1L);
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

    public long getLikeDelta() {
        return likeDelta;
    }

    public void setLikeDelta(long likeDelta) {
        this.likeDelta = likeDelta;
    }

    public long getCommentDelta() {
        return commentDelta;
    }

    public void setCommentDelta(long commentDelta) {
        this.commentDelta = commentDelta;
    }

    public long getShareDelta() {
        return shareDelta;
    }

    public void setShareDelta(long shareDelta) {
        this.shareDelta = shareDelta;
    }

    public long getFollowDelta() {
        return followDelta;
    }

    public void setFollowDelta(long followDelta) {
        this.followDelta = followDelta;
    }
}
