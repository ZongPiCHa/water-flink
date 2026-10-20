package com.zongpicha.waterflink.model;

import java.io.Serializable;

/** A comment joined with the delete issued by its own author shortly afterwards. */
public class CommentRetraction implements Serializable {

    private static final long serialVersionUID = 1L;

    private String userId;
    private String commentId;
    private String projectId;
    private String authorUserId;
    private long commentedAt;
    private long deletedAt;

    public CommentRetraction() {
    }

    public CommentRetraction(EngagementEvent comment, EngagementEvent delete) {
        this.userId = comment.getActorUserId();
        this.commentId = comment.getCommentId();
        this.projectId = comment.getProjectId();
        this.authorUserId = comment.getTargetUserId();
        this.commentedAt = comment.getTimestamp();
        this.deletedAt = delete.getTimestamp();
    }

    public long lifetimeMillis() {
        return deletedAt - commentedAt;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCommentId() {
        return commentId;
    }

    public void setCommentId(String commentId) {
        this.commentId = commentId;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getAuthorUserId() {
        return authorUserId;
    }

    public void setAuthorUserId(String authorUserId) {
        this.authorUserId = authorUserId;
    }

    public long getCommentedAt() {
        return commentedAt;
    }

    public void setCommentedAt(long commentedAt) {
        this.commentedAt = commentedAt;
    }

    public long getDeletedAt() {
        return deletedAt;
    }

    public void setDeletedAt(long deletedAt) {
        this.deletedAt = deletedAt;
    }
}
