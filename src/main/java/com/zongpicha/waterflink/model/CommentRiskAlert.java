package com.zongpicha.waterflink.model;

import java.io.Serializable;

/** Emitted when one user repeatedly posts and quickly retracts comments. */
public class CommentRiskAlert implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;
    private String firstCommentId;
    private long firstCommentAt;
    private long lastDeletedAt;
    private int retractCount;
    private int projectCount;
    private int authorCount;
    private long avgLifetimeMillis;
    private long maxLifetimeMillis;
    private String commentIds;
    private String projectIds;

    public CommentRiskAlert() {
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getFirstCommentId() {
        return firstCommentId;
    }

    public void setFirstCommentId(String firstCommentId) {
        this.firstCommentId = firstCommentId;
    }

    public long getFirstCommentAt() {
        return firstCommentAt;
    }

    public void setFirstCommentAt(long firstCommentAt) {
        this.firstCommentAt = firstCommentAt;
    }

    public long getLastDeletedAt() {
        return lastDeletedAt;
    }

    public void setLastDeletedAt(long lastDeletedAt) {
        this.lastDeletedAt = lastDeletedAt;
    }

    public int getRetractCount() {
        return retractCount;
    }

    public void setRetractCount(int retractCount) {
        this.retractCount = retractCount;
    }

    public int getProjectCount() {
        return projectCount;
    }

    public void setProjectCount(int projectCount) {
        this.projectCount = projectCount;
    }

    public int getAuthorCount() {
        return authorCount;
    }

    public void setAuthorCount(int authorCount) {
        this.authorCount = authorCount;
    }

    public long getAvgLifetimeMillis() {
        return avgLifetimeMillis;
    }

    public void setAvgLifetimeMillis(long avgLifetimeMillis) {
        this.avgLifetimeMillis = avgLifetimeMillis;
    }

    public long getMaxLifetimeMillis() {
        return maxLifetimeMillis;
    }

    public void setMaxLifetimeMillis(long maxLifetimeMillis) {
        this.maxLifetimeMillis = maxLifetimeMillis;
    }

    public String getCommentIds() {
        return commentIds;
    }

    public void setCommentIds(String commentIds) {
        this.commentIds = commentIds;
    }

    public String getProjectIds() {
        return projectIds;
    }

    public void setProjectIds(String projectIds) {
        this.projectIds = projectIds;
    }
}
