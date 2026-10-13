package com.zongpicha.waterflink.model;

import java.io.Serializable;

public class EngagementAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    private long likes;
    private long comments;
    private long shares;
    private long follows;

    public EngagementAccumulator() {
    }

    public void add(MetricDelta delta) {
        likes += delta.getLikeDelta();
        comments += delta.getCommentDelta();
        shares += delta.getShareDelta();
        follows += delta.getFollowDelta();
    }

    public void merge(EngagementAccumulator other) {
        likes += other.likes;
        comments += other.comments;
        shares += other.shares;
        follows += other.follows;
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
