package com.zongpicha.waterflink.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EngagementAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    private String targetUserId;
    private String projectId;
    private long likes;
    private long comments;
    private long shares;
    private long follows;
    private Map<String, Long> extraMetrics;

    public Map<String, Long> getExtraMetrics() {
        return extraMetrics;
    }

    public void setExtraMetrics(Map<String, Long> extraMetrics) {
        this.extraMetrics = extraMetrics;
    }

    public EngagementAccumulator() {
    }

    public void add(MetricDelta delta) {
        if (targetUserId == null) {
            targetUserId = delta.getTargetUserId();
            projectId = delta.getProjectId();
        }
        likes += delta.getLikeDelta();
        comments += delta.getCommentDelta();
        shares += delta.getShareDelta();
        follows += delta.getFollowDelta();
        if (delta.getMetricName() != null) {
            if (extraMetrics == null) {
                extraMetrics = new HashMap<>();
            }
            extraMetrics.merge(delta.getMetricName(), 1L, Long::sum);
        }
    }

    public void merge(EngagementAccumulator other) {
        if (targetUserId == null) {
            targetUserId = other.targetUserId;
            projectId = other.projectId;
        }
        likes += other.likes;
        comments += other.comments;
        shares += other.shares;
        follows += other.follows;
        if (other.extraMetrics != null) {
            if (extraMetrics == null) {
                extraMetrics = new HashMap<>();
            }
            other.extraMetrics.forEach((name, count) -> extraMetrics.merge(name, count, Long::sum));
        }
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

    public long getLikes() {
        return likes;
    }

    public void setLikes(long likes) {
        this.likes = likes;
    }

    public long getComments() {
        return comments;
    }

    public void setComments(long comments) {
        this.comments = comments;
    }

    public long getShares() {
        return shares;
    }

    public void setShares(long shares) {
        this.shares = shares;
    }

    public long getFollows() {
        return follows;
    }

    public void setFollows(long follows) {
        this.follows = follows;
    }
}
