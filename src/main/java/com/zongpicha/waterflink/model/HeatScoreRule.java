package com.zongpicha.waterflink.model;

import java.io.Serializable;
import java.util.Map;

public class HeatScoreRule implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final HeatScoreRule DEFAULT =
            new HeatScoreRule(1L, 1000L, 3000L, 5000L, 4000L);

    private long version;
    private long likeWeight;
    private long commentWeight;
    private long shareWeight;
    private long followWeight;
    private Map<String, Long> weights;

    public HeatScoreRule() {
    }

    public HeatScoreRule(
            long version,
            long likeWeight,
            long commentWeight,
            long shareWeight,
            long followWeight) {
        this.version = version;
        this.likeWeight = likeWeight;
        this.commentWeight = commentWeight;
        this.shareWeight = shareWeight;
        this.followWeight = followWeight;
    }

    public long calculate(EngagementStats stats) {
        if (weights != null) {
            long score = 0L;
            for (Map.Entry<String, Long> entry : weights.entrySet()) {
                score = Math.addExact(score,
                        Math.multiplyExact(stats.metricValue(entry.getKey()), entry.getValue()));
            }
            return score;
        }
        return stats.getLikes() * likeWeight
                + stats.getComments() * commentWeight
                + stats.getShares() * shareWeight
                + stats.getFollows() * followWeight;
    }

    public Map<String, Long> getWeights() {
        return weights;
    }

    public void setWeights(Map<String, Long> weights) {
        this.weights = weights;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getLikeWeight() {
        return likeWeight;
    }

    public void setLikeWeight(long likeWeight) {
        this.likeWeight = likeWeight;
    }

    public long getCommentWeight() {
        return commentWeight;
    }

    public void setCommentWeight(long commentWeight) {
        this.commentWeight = commentWeight;
    }

    public long getShareWeight() {
        return shareWeight;
    }

    public void setShareWeight(long shareWeight) {
        this.shareWeight = shareWeight;
    }

    public long getFollowWeight() {
        return followWeight;
    }

    public void setFollowWeight(long followWeight) {
        this.followWeight = followWeight;
    }
}
