package com.zongpicha.waterflink.model;

import java.io.Serializable;

public class UserEngagementStats implements Serializable {

    private static final long serialVersionUID = 1L;

    private String targetUserId;
    private long windowStart;
    private long windowEnd;
    private long likes;
    private long comments;
    private long shares;
    private long follows;

    public UserEngagementStats() {
    }

    public UserEngagementStats(
            String targetUserId,
            long windowStart,
            long windowEnd,
            long likes,
            long comments,
            long shares,
            long follows) {
        this.targetUserId = targetUserId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.likes = likes;
        this.comments = comments;
        this.shares = shares;
        this.follows = follows;
    }

    public String getTargetUserId() {
        return targetUserId;
    }

    public void setTargetUserId(String targetUserId) {
        this.targetUserId = targetUserId;
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
}
