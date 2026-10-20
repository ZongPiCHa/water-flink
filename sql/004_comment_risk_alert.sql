USE water_analytics;

-- Written by the water-flink comment risk branch (interval join + CEP).
-- Times are Asia/Shanghai local, like the engagement stats tables.
CREATE TABLE IF NOT EXISTS comment_risk_alert (
    user_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    first_comment_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    first_comment_at DATETIME(3) NOT NULL,
    last_deleted_at DATETIME(3) NOT NULL,
    retract_count INT UNSIGNED NOT NULL,
    project_count INT UNSIGNED NOT NULL,
    author_count INT UNSIGNED NOT NULL,
    avg_lifetime_ms BIGINT NOT NULL,
    max_lifetime_ms BIGINT NOT NULL,
    comment_ids VARCHAR(1024) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    project_ids VARCHAR(1024) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id, first_comment_id),
    KEY idx_comment_risk_alert_time (last_deleted_at)
) ENGINE=InnoDB;
