package com.zongpicha.waterflink.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

public class EngagementStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private StatsScope scope;
    private WindowType windowType;
    private String userId;
    private String projectId;
    private long windowStart;
    private long windowEnd;
    private long likes;
    private long comments;
    private long shares;
    private long follows;
    private long heatScore;
    private long scoreVersion;
    private String userNickname;
    private String projectName;
    private LocalDateTime projectPublishedAt;
    private Map<String, Long> extraMetrics;

    public Map<String, Long> getExtraMetrics() {
        return extraMetrics;
    }

    public void setExtraMetrics(Map<String, Long> extraMetrics) {
        this.extraMetrics = extraMetrics;
    }

    public long metricValue(String name) {
        return switch (name) {
            case "like" -> likes;
            case "comment" -> comments;
            case "share" -> shares;
            case "follow" -> follows;
            default -> extraMetrics == null ? 0L : extraMetrics.getOrDefault(name, 0L);
        };
    }

    public EngagementStats() {
    }

    public EngagementStats(
            StatsScope scope,
            WindowType windowType,
            String userId,
            String projectId,
            long windowStart,
            long windowEnd,
            long likes,
            long comments,
            long shares,
            long follows) {
        this.scope = scope;
        this.windowType = windowType;
        this.userId = userId;
        this.projectId = projectId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.likes = likes;
        this.comments = comments;
        this.shares = shares;
        this.follows = follows;
    }

    public StatsScope getScope() {
        return scope;
    }

    public void setScope(StatsScope scope) {
        this.scope = scope;
    }

    public WindowType getWindowType() {
        return windowType;
    }

    public void setWindowType(WindowType windowType) {
        this.windowType = windowType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
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

    public long getHeatScore() {
        return heatScore;
    }

    public void setHeatScore(long heatScore) {
        this.heatScore = heatScore;
    }

    public long getScoreVersion() {
        return scoreVersion;
    }

    public void setScoreVersion(long scoreVersion) {
        this.scoreVersion = scoreVersion;
    }

    public String getUserNickname() {
        return userNickname;
    }

    public void setUserNickname(String userNickname) {
        this.userNickname = userNickname;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public LocalDateTime getProjectPublishedAt() {
        return projectPublishedAt;
    }

    public void setProjectPublishedAt(LocalDateTime projectPublishedAt) {
        this.projectPublishedAt = projectPublishedAt;
    }
}
